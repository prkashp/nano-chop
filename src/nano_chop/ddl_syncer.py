"""DDL Syncer: detects schema changes and updates IMPORT tables."""

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
import snowflake.connector

from .sql_builder import (
    build_pk_join_condition, build_pk_partition_expr, build_column_select,
    TYPE_PRIORITY
)
from .models import DDLControlRecord
from .event_logger import log_ddl_sync_success


def _parse_primary_keys(cursor) -> list[str]:
    """
    Parse primary keys from OBJECT_KEYS result.

    Args:
        cursor: Snowflake cursor with executed query returning OBJECT_KEYS(RECORD_METADATA:key)

    Returns:
        List of primary key column names
    """
    if not cursor.fetchone():
        return []

    row = cursor.fetchone()
    if row is None or row[0] is None:
        return []

    keys_json = row[0]
    # OBJECT_KEYS returns an ARRAY in Snowflake, which comes as a list in connector
    if isinstance(keys_json, str):
        try:
            return json.loads(keys_json)
        except json.JSONDecodeError:
            return []
    elif isinstance(keys_json, list):
        return keys_json
    else:
        return []


def _detect_columns(
    conn: snowflake.connector.SnowflakeConnection,
    table_name: str,
    target_schema: str,
    run_mode: str = '',
    source_schema: str = 'LANDING'
) -> dict[str, tuple[Optional[str], Optional[str]]]:
    """
    Detect columns in landing table and compare with target table.

    Returns dict: {column_name: (landing_type, target_type)}
    """
    cursor = conn.cursor()

    # Set filter based on run_mode
    if run_mode.upper() == "FULL":
        filter_clause = "1=1"
    elif run_mode.upper() == "TEST":
        filter_clause = (
            f"DW_CREATED_DATE >= "
            f"(SELECT MAX(DATE(DW_CREATED_DATE)) FROM {source_schema}.{table_name})"
        )
    else:
        # Default: last 5 days
        filter_clause = f"DATE(DW_CREATED_DATE) >= DATEADD(DAY, -5, CURRENT_DATE())"

    sql = f"""
    WITH column_samples AS (
        SELECT
            f.value::STRING AS COLUMN_NAME,
            MAX(RECORD_CONTENT[f.value::STRING]) as sample_value
        FROM {source_schema}.{table_name},
        LATERAL FLATTEN(input => OBJECT_KEYS(RECORD_CONTENT)) f
        WHERE {filter_clause}
          AND f.value::VARCHAR NOT IN ('__op', '__deleted', '__source_ts_ms', '__lsn', '__table')
        GROUP BY 1
    )
    SELECT DISTINCT
        cs.COLUMN_NAME,
        CASE
            WHEN c.COLUMN_NAME IS NULL THEN
                CASE WHEN TYPEOF(cs.sample_value) in ('NULL','NULL_VALUE') THEN 'VARCHAR'
                    ELSE TYPEOF(cs.sample_value) END
            ELSE IFF(c.DATA_TYPE='NUMBER', CONCAT(c.DATA_TYPE,'(',c.numeric_precision,',',c.numeric_scale,')'), c.DATA_TYPE)
        END AS LANDING_DATA_TYPE,
        IFF(c.DATA_TYPE='NUMBER', CONCAT(c.DATA_TYPE,'(',c.numeric_precision,',',c.numeric_scale,')'), c.DATA_TYPE) AS TARGET_DATA_TYPE
    FROM column_samples cs
    LEFT JOIN INFORMATION_SCHEMA.COLUMNS c
        ON c.COLUMN_NAME = UPPER(cs.COLUMN_NAME)
        AND c.table_name = '{table_name}'
        AND c.table_schema = '{target_schema}'
    ORDER BY cs.COLUMN_NAME
    """

    cursor.execute(sql)
    result = {}

    for row in cursor.fetchall():
        col_name = row[0]
        landing_type = row[1]
        target_type = row[2]
        result[col_name] = (landing_type, target_type)

    cursor.close()
    return result


def sync_table(
    conn: snowflake.connector.SnowflakeConnection,
    table_name: str,
    target_schema: str,
    run_mode: str = '',
    source_schema: str = 'LANDING',
    admin_schema: str = 'ADMIN'
) -> list[str]:
    """
    Synchronize DDL for a single table.

    Args:
        conn: Snowflake connection
        table_name: Landing table name
        target_schema: Target schema (e.g., 'IMPORT')
        run_mode: "" (last 5 days), "FULL", or "TEST"
        source_schema: Landing schema name
        admin_schema: Admin schema name

    Returns:
        List of result messages
    """
    results = []
    cursor = conn.cursor()

    try:
        # Step 1: Check if landing table has data
        check_landing_sql = f"""
        SELECT COUNT(1)
        FROM {source_schema}.{table_name}
        WHERE RECORD_METADATA:key IS NOT NULL
        """

        cursor.execute(check_landing_sql)
        landing_count = cursor.fetchone()[0]

        if landing_count == 0:
            results.append(f"No records available in {table_name} table")
            return results

        # Step 2: Check if control table has entry
        check_control_sql = (
            f"SELECT 1 FROM {admin_schema}.ddl_control WHERE table_name = '{table_name}'"
        )
        cursor.execute(check_control_sql)
        control_exists = cursor.fetchone() is not None
        first_time = not control_exists

        primary_keys = []
        primary_keys_str = ""
        primary_keys_partition = ""

        # Step 3: If first time, get primary keys and init control entry
        if first_time:
            get_pk_sql = f"""
            SELECT OBJECT_KEYS(RECORD_METADATA:key)
            FROM {source_schema}.{table_name}
            WHERE RECORD_METADATA:key IS NOT NULL
            ORDER BY DW_CREATED_DATE DESC
            LIMIT 1
            """

            cursor.execute(get_pk_sql)
            pk_result = cursor.fetchone()

            if pk_result and pk_result[0]:
                # OBJECT_KEYS returns an array
                pk_array = pk_result[0]
                if isinstance(pk_array, str):
                    primary_keys = json.loads(pk_array)
                else:
                    primary_keys = pk_array

                primary_keys = [str(k).strip() for k in primary_keys]
                primary_keys_str = build_pk_join_condition(primary_keys)
                primary_keys_partition = build_pk_partition_expr(primary_keys)

            init_control_sql = f"""
            INSERT INTO {admin_schema}.ddl_control (
                table_name, primary_keys, primary_keys_str, primary_keys_part,
                created_at, created_by
            ) VALUES (
                '{table_name}',
                '{", ".join(primary_keys)}',
                '{primary_keys_str}',
                '{primary_keys_partition}',
                CURRENT_TIMESTAMP(),
                CURRENT_USER()
            )
            """

            cursor.execute(init_control_sql)
            conn.commit()
            results.append(f"Initialized ddl_control entry for {table_name}")

        # Step 4: Detect columns
        columns = _detect_columns(
            conn, table_name, target_schema, run_mode, source_schema
        )

        if not columns:
            results.append("No columns detected")
            return results

        # Step 5: Build select, update, insert, delete strings
        select_cols = []
        update_cols = []
        insert_cols = []
        delete_cols = []
        new_columns = []

        for col_name, (landing_type, target_type) in columns.items():
            col_upper = col_name.upper()
            is_pk = any(pk.upper() == col_upper for pk in primary_keys)

            # Build SELECT for this column
            data_type = landing_type or target_type or 'VARCHAR'
            select_cols.append(build_column_select(col_name, data_type, is_pk))

            # UPDATE/INSERT
            update_cols.append(f"a.{col_name} = b.{col_name}")
            insert_cols.append(f"b.{col_name}")

            # Check if it's a new column (not in target)
            if target_type is None and landing_type:
                if col_name not in {c[0]: c[1] for c in new_columns}:
                    # Apply type priority
                    data_type_upper = landing_type.upper()
                    if col_name not in [c[0] for c in new_columns] or \
                       (TYPE_PRIORITY.get(data_type_upper, 0) >
                        TYPE_PRIORITY.get(
                            [c[1] for c in new_columns if c[0] == col_name][0].upper() if
                            col_name in [c[0] for c in new_columns] else 'VARCHAR',
                            0
                        )):
                        new_columns = [(c[0], c[1]) for c in new_columns if c[0] != col_name]
                        new_columns.append((col_name, landing_type))

            # DELETE condition (chgts column)
            if col_name.lower() == 'chgts':
                delete_cols.append("a.chgts < b.record_created_date")

        if not delete_cols:
            delete_cols.append("1=1")

        select_str = ", ".join(select_cols)
        update_str = ", ".join(update_cols)
        insert_str = ", ".join(insert_cols)
        delete_str = delete_cols[0]

        update_required = len(new_columns) > 0

        # Step 6: Update control table if needed
        if update_required or first_time or run_mode.upper() in ('FULL', 'TEST'):
            update_control_sql = f"""
            UPDATE {admin_schema}.ddl_control
            SET
                update_str = '{update_str}',
                select_str = '{select_str}',
                insert_str = '{insert_str}',
                delete_str = '{delete_str}',
                updated_at = CURRENT_TIMESTAMP(),
                updated_by = CURRENT_USER()
            WHERE table_name = '{table_name}'
            """

            cursor.execute(update_control_sql)
            conn.commit()
            results.append(f"Updated ddl_control for {table_name} with {len(columns)} columns")

        # Step 7: Alter table if new columns
        if new_columns:
            alter_stmts = []
            for col_name, col_type in new_columns:
                alter_stmts.append(f'"{col_name}" {col_type}')

            alter_sql = (
                f'ALTER TABLE "{target_schema}"."{table_name}" '
                f'ADD COLUMN IF NOT EXISTS {", ".join(alter_stmts)}'
            )

            cursor.execute(alter_sql)
            conn.commit()
            results.append(f"Added {len(new_columns)} new columns to {target_schema}.{table_name}")
        else:
            results.append("No new columns to add")

        return results

    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"Error synchronizing DDL for {table_name}: {str(e)}")
    finally:
        cursor.close()


def sync_all_tables(
    config: dict,
    env: str,
    groups: Optional[list[str]] = None,
    run_mode: str = '',
    max_workers: int = 4
) -> dict[str, list[str]]:
    """
    Synchronize DDL for all tables (or subset by groups).

    Args:
        config: Parsed YAML config dict
        env: Environment name (e.g., 'prod')
        groups: Optional list of table groups to process (default: all)
        run_mode: "", "FULL", or "TEST"
        max_workers: ThreadPoolExecutor max_workers

    Returns:
        Dict mapping table_name -> list of result messages
    """
    from .connection import get_connection

    env_config = config['environments'][env]
    conn = get_connection(env_config)

    target_schema = env_config.get('target_schema', 'IMPORT')
    source_schema = env_config.get('landing_schema', 'LANDING')
    admin_schema = config.get('admin_schema', 'ADMIN')

    # Build list of tables to process
    tables_to_sync = []
    all_tables = config.get('tables', {})

    if groups:
        for group in groups:
            if group in all_tables:
                tables_to_sync.extend(all_tables[group])
    else:
        for group_tables in all_tables.values():
            tables_to_sync.extend(group_tables)

    results = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                sync_table,
                conn,
                table,
                target_schema,
                run_mode,
                source_schema,
                admin_schema
            ): table
            for table in tables_to_sync
        }

        for future in as_completed(futures):
            table_name = futures[future]
            try:
                results[table_name] = future.result()
            except Exception as e:
                results[table_name] = [f"ERROR: {str(e)}"]

    conn.close()
    return results
