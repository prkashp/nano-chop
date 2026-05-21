"""Event logging to Snowflake ADMIN.SNOWFLAKE_EVENTS table."""

import json
from typing import Any, Optional
import snowflake.connector


def log_event(
    conn: snowflake.connector.SnowflakeConnection,
    level: str,
    status: str,
    attributes: dict[str, Any],
    schema: str = 'ADMIN'
) -> None:
    """
    Log an event to ADMIN.SNOWFLAKE_EVENTS table.

    Args:
        conn: Snowflake connection
        level: Log level ('INFO', 'ERROR', 'WARNING')
        status: Status value ('SUCCESS', 'FAILED', 'SKIPPED')
        attributes: Dict with event details (table_name, schema, counts, error, etc.)
        schema: Admin schema name (default 'ADMIN')

    Raises:
        snowflake.connector.Error: If insert fails
    """
    try:
        cursor = conn.cursor()

        # Sanitize JSON to avoid SQL injection
        json_str = json.dumps(attributes)
        # Escape single quotes for SQL
        json_str = json_str.replace("'", "''")

        sql = f"""
        INSERT INTO {schema}.SNOWFLAKE_EVENTS (TIMESTAMP, RECORD_TYPE, VALUE, RECORD_ATTRIBUTES)
        VALUES (CURRENT_TIMESTAMP(), '{level}', '{status}', PARSE_JSON('{json_str}'))
        """

        cursor.execute(sql)
        conn.commit()
        cursor.close()

    except Exception as e:
        raise snowflake.connector.Error(f"Failed to log event: {e}")


def log_ddl_sync_success(
    conn: snowflake.connector.SnowflakeConnection,
    table_name: str,
    new_columns: int,
    duration_seconds: float,
    schema: str = 'ADMIN'
) -> None:
    """
    Log successful DDL sync event.

    Args:
        conn: Snowflake connection
        table_name: Table being synced
        new_columns: Number of new columns added
        duration_seconds: Time taken
        schema: Admin schema name
    """
    log_event(
        conn,
        level='INFO',
        status='SUCCESS',
        attributes={
            'table_name': table_name,
            'operation': 'DDL_SYNC',
            'new_columns': new_columns,
            'duration_seconds': duration_seconds
        },
        schema=schema
    )


def log_streams_processor_success(
    conn: snowflake.connector.SnowflakeConnection,
    table_name: str,
    schema_name: str,
    records_processed: int,
    insert_count: int,
    update_count: int,
    delete_count: int,
    last_landing_key: float,
    duration_seconds: float,
    schema: str = 'ADMIN'
) -> None:
    """
    Log successful streams processor event.

    Args:
        conn: Snowflake connection
        table_name: Table being processed
        schema_name: Target schema name
        records_processed: Total records processed
        insert_count: Number of inserts
        update_count: Number of updates
        delete_count: Number of deletes
        last_landing_key: Max landing_key processed
        duration_seconds: Time taken
        schema: Admin schema name
    """
    log_event(
        conn,
        level='INFO',
        status='SUCCESS',
        attributes={
            'table_name': table_name,
            'schema': schema_name,
            'operation': 'STREAMS_PROCESSOR',
            'record_processed': records_processed,
            'insert_count': insert_count,
            'update_count': update_count,
            'delete_count': delete_count,
            'last_landing_key': last_landing_key,
            'duration_seconds': duration_seconds
        },
        schema=schema
    )


def log_streams_processor_failure(
    conn: snowflake.connector.SnowflakeConnection,
    table_name: str,
    schema_name: str,
    error_message: str,
    records_processed: Optional[int] = None,
    insert_count: Optional[int] = None,
    update_count: Optional[int] = None,
    delete_count: Optional[int] = None,
    schema: str = 'ADMIN'
) -> None:
    """
    Log failed streams processor event.

    Args:
        conn: Snowflake connection
        table_name: Table being processed
        schema_name: Target schema name
        error_message: Error details
        records_processed: Partial record count if available
        insert_count: Partial insert count if available
        update_count: Partial update count if available
        delete_count: Partial delete count if available
        schema: Admin schema name
    """
    attributes = {
        'table_name': table_name,
        'schema': schema_name,
        'operation': 'STREAMS_PROCESSOR',
        'error': error_message,
        'record_processed': records_processed or 0,
        'insert_count': insert_count or 0,
        'update_count': update_count or 0,
        'delete_count': delete_count or 0,
    }

    log_event(
        conn,
        level='ERROR',
        status='FAILED',
        attributes=attributes,
        schema=schema
    )


def get_last_landing_key(
    conn: snowflake.connector.SnowflakeConnection,
    table_name: str,
    schema_name: str,
    hours_back: int = 6,
    schema: str = 'ADMIN'
) -> float:
    """
    Get the last successfully processed landing_key for a table.

    Args:
        conn: Snowflake connection
        table_name: Table name
        schema_name: Target schema name
        hours_back: Look back this many hours for the last event
        schema: Admin schema name

    Returns:
        Last landing_key (or 0 if no previous run found)
    """
    try:
        cursor = conn.cursor()

        sql = f"""
        SELECT COALESCE(RECORD_ATTRIBUTES:'last_landing_key'::FLOAT, 0)
        FROM {schema}.SNOWFLAKE_EVENTS
        WHERE TIMESTAMP >= DATEADD('hour', -{hours_back}, CURRENT_TIMESTAMP())
          AND RECORD_TYPE = 'INFO'
          AND VALUE = 'SUCCESS'
          AND RECORD_ATTRIBUTES:'table_name' = '{table_name}'
          AND RECORD_ATTRIBUTES:'schema' = '{schema_name}'
        ORDER BY TIMESTAMP DESC
        LIMIT 1
        """

        cursor.execute(sql)
        result = cursor.fetchone()
        cursor.close()

        return result[0] if result else 0.0

    except Exception as e:
        raise snowflake.connector.Error(f"Failed to get last landing key: {e}")
