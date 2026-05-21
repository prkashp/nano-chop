"""Streams Processor: CDC load from LANDING to IMPORT schema."""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
import snowflake.connector

from .sql_builder import (
    build_temp_batch_sql, build_update_sql, build_insert_sql, build_delete_sql,
    apply_debezium_mapping
)
from .models import DDLControlRecord
from .event_logger import log_streams_processor_success, log_streams_processor_failure, get_last_landing_key
from .ddl_syncer import sync_table


def process_table(
    conn: snowflake.connector.SnowflakeConnection,
    table_name: str,
    schema: str,
    bypass_landing_key: float = -1,
    source_schema: str = 'LANDING',
    tmp_schema: str = 'TMP',
    admin_schema: str = 'ADMIN'
) -> list[str]:
    """
    Process CDC stream for a single table.

    Args:
        conn: Snowflake connection
        table_name: Table name
        schema: Target schema (e.g., 'IMPORT')
        bypass_landing_key: Override last key (default -1 uses last logged key)
        source_schema: Landing schema name
        tmp_schema: Temp schema name
        admin_schema: Admin schema name

    Returns:
        List of result messages

    Raises:
        Exception: If processing fails
    """
    results = []
    cursor = conn.cursor()
    start_time = time.time()

    insert_count = 0
    update_count = 0
    delete_count = 0
    last_landing_key = 0
    total_count = 0

    try:
        # Step 1: Get last landing key from events
        max_landing_key = get_last_landing_key(conn, table_name, schema, admin_schema=admin_schema)
        effective_key = bypass_landing_key if bypass_landing_key > -1 else max_landing_key
        results.append(f"Last Landing key: {max_landing_key} | BYPASS_LANDING_KEY: {bypass_landing_key}")

        # Step 2: Check if landing table has new data
        check_landing_sql = f"""
        SELECT COUNT(1)
        FROM {source_schema}.{table_name}
        WHERE {table_name}_landing_key > {effective_key}
        """

        cursor.execute(check_landing_sql)
        landing_count = cursor.fetchone()[0]

        if landing_count == 0:
            results.append(f"No new data in landing table for {table_name}, skipping")

            # Still log if we had a previous key
            if max_landing_key > 0:
                log_streams_processor_success(
                    conn, table_name, schema, 0, 0, 0, 0, max_landing_key,
                    time.time() - start_time, admin_schema
                )

            return results

        # Step 3: Get DDL control info
        get_ddl_sql = f"""
        SELECT
            primary_keys, primary_keys_str, primary_keys_part,
            select_str, insert_str, update_str, delete_str
        FROM {admin_schema}.ddl_control
        WHERE table_name = '{table_name}'
        LIMIT 1
        """

        cursor.execute(get_ddl_sql)
        ddl_row = cursor.fetchone()

        if ddl_row is None:
            # Try to sync DDL first
            results.append(f"No DDL control found for {table_name}, attempting sync...")
            sync_results = sync_table(conn, table_name, schema, 'TEST', source_schema, admin_schema)
            results.extend(sync_results)

            # Retry DDL lookup
            cursor.execute(get_ddl_sql)
            ddl_row = cursor.fetchone()

            if ddl_row is None:
                results.append(f"No active configuration found for {table_name}, skipping")
                return results

        primary_keys = ddl_row[0].split(', ')
        primary_keys_str = ddl_row[1]
        primary_keys_part = ddl_row[2]
        select_str = ddl_row[3]
        insert_str = ddl_row[4]
        update_str = ddl_row[5]
        delete_str = ddl_row[6]

        # Step 4: Check for debezium unavailable values
        check_debezium_sql = f"""
        SELECT DISTINCT o.KEY
        FROM {source_schema}.{table_name} c,
        LATERAL FLATTEN(INPUT => c.RECORD_CONTENT) o
        WHERE c.{table_name}_landing_key > {effective_key}
          AND o.VALUE::VARCHAR = '__debezium_unavailable_value'
        """

        cursor.execute(check_debezium_sql)
        unavailable_cols = [row[0] for row in cursor.fetchall()]

        # Step 5: Apply debezium mapping if needed
        mapped_select_str = select_str
        mapped_update_str = update_str

        if unavailable_cols:
            mapped_select_str, mapped_update_str = apply_debezium_mapping(
                select_str, update_str, unavailable_cols, primary_keys_part
            )

        # Step 6: Build all SQL statements
        temp_batch_sql = build_temp_batch_sql(
            table_name, source_schema, primary_keys_part, mapped_select_str,
            effective_key, tmp_schema
        )

        update_sql = build_update_sql(table_name, schema, primary_keys_str, mapped_update_str, tmp_schema)
        insert_sql = build_insert_sql(table_name, schema, primary_keys_str, insert_str, tmp_schema)
        delete_sql = build_delete_sql(table_name, schema, primary_keys_str, delete_str, tmp_schema)

        # Step 7: Execute in transaction
        cursor.execute("BEGIN")

        # Create temp batch table
        cursor.execute(temp_batch_sql)

        # Update existing rows
        cursor.execute(update_sql)
        update_count = cursor.rowcount

        # Insert new rows
        cursor.execute(insert_sql)
        insert_count = cursor.rowcount

        # Soft delete
        cursor.execute(delete_sql)
        delete_count = cursor.rowcount

        total_count = insert_count + update_count + delete_count

        # Get last landing key from batch
        get_last_key_sql = f"SELECT MAX({table_name}_landing_key) FROM {tmp_schema}.{table_name}_BATCH"
        cursor.execute(get_last_key_sql)
        key_row = cursor.fetchone()
        last_landing_key = key_row[0] if key_row and key_row[0] else effective_key

        cursor.execute("COMMIT")

        results.append(
            f"Records processed: Inserts={insert_count}, Updates={update_count}, "
            f"Deletes={delete_count}, Total={total_count}"
        )

        # Log success
        log_streams_processor_success(
            conn, table_name, schema, total_count, insert_count, update_count,
            delete_count, last_landing_key, time.time() - start_time, admin_schema
        )

        return results

    except Exception as e:
        cursor.execute("ROLLBACK")
        error_msg = str(e)

        # Log failure
        try:
            log_streams_processor_failure(
                conn, table_name, schema, error_msg, total_count, insert_count,
                update_count, delete_count, admin_schema
            )
        except:
            pass

        raise RuntimeError(f"Error processing {table_name}: {error_msg}")

    finally:
        cursor.close()


def process_all_tables(
    config: dict,
    env: str,
    groups: Optional[list[str]] = None,
    bypass_landing_key: float = -1,
    max_workers: Optional[int] = None
) -> dict[str, list[str]]:
    """
    Process CDC streams for all tables (or subset by groups).

    Args:
        config: Parsed YAML config dict
        env: Environment name (e.g., 'prod')
        groups: Optional list of table groups to process (default: all)
        bypass_landing_key: Override last key (-1 uses last logged)
        max_workers: ThreadPoolExecutor max_workers (default from config)

    Returns:
        Dict mapping table_name -> list of result messages
    """
    from .connection import get_connection

    env_config = config['environments'][env]
    conn = get_connection(env_config)

    target_schema = env_config.get('target_schema', 'IMPORT')
    source_schema = env_config.get('landing_schema', 'LANDING')
    tmp_schema = env_config.get('tmp_schema', 'TMP')
    admin_schema = config.get('admin_schema', 'ADMIN')

    # Get max_workers from config
    if max_workers is None:
        streams_config = config.get('streams_processor', {})
        max_workers = streams_config.get('max_workers', 8)

    # Build list of tables to process
    tables_to_process = []
    all_tables = config.get('tables', {})

    if groups:
        for group in groups:
            if group in all_tables:
                tables_to_process.extend(all_tables[group])
    else:
        for group_tables in all_tables.values():
            tables_to_process.extend(group_tables)

    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                process_table,
                conn,
                table,
                target_schema,
                bypass_landing_key,
                source_schema,
                tmp_schema,
                admin_schema
            ): table
            for table in tables_to_process
        }

        for future in as_completed(futures):
            table_name = futures[future]
            try:
                results[table_name] = future.result()
            except Exception as e:
                results[table_name] = [f"ERROR: {str(e)}"]

    conn.close()
    return results
