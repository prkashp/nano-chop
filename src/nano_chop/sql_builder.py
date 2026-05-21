"""SQL string builders for DDL and CDC processing."""

import re
from typing import Optional, Tuple

TYPE_PRIORITY = {
    'DOUBLE': 4,
    'FLOAT': 3,
    'INT': 2,
    'TEXT': 2,
    'VARCHAR': 1,
}


def build_pk_join_condition(primary_keys: list[str]) -> str:
    """
    Build primary key join condition.

    Args:
        primary_keys: List of primary key column names

    Returns:
        SQL join condition, e.g., "a.id = b.id AND a.tenant_id = b.tenant_id"
    """
    if not primary_keys:
        return "1=1"

    conditions = [f"a.{pk.strip()} = b.{pk.strip()}" for pk in primary_keys]
    return " AND ".join(conditions)


def build_pk_partition_expr(primary_keys: list[str]) -> str:
    """
    Build primary key partition expression for window functions.

    Args:
        primary_keys: List of primary key column names

    Returns:
        COALESCE expression, e.g., "COALESCE(...key:id, ...CONTENT:id), ..."
    """
    if not primary_keys:
        return "1"

    parts = []
    for pk in primary_keys:
        pk = pk.strip()
        parts.append(
            f"COALESCE(RECORD_METADATA:key:{pk}, RECORD_CONTENT:{pk})"
        )

    return ", ".join(parts)


def build_column_select(
    col_name: str,
    data_type: Optional[str],
    is_primary_key: bool = False
) -> str:
    """
    Build SELECT clause for a single column.

    Args:
        col_name: Column name
        data_type: Data type (e.g., 'VARCHAR', 'INT', 'DOUBLE')
        is_primary_key: Whether this is a primary key column

    Returns:
        SQL fragment, e.g., "RECORD_CONTENT:col_name::VARCHAR as col_name"
    """
    if data_type is None:
        data_type = 'VARCHAR'

    col_name = col_name.strip()
    data_type = data_type.upper()

    if is_primary_key:
        return (
            f"COALESCE(RECORD_METADATA:key:{col_name}::{data_type}, "
            f"RECORD_CONTENT:{col_name}::{data_type}) as {col_name}"
        )
    else:
        return f"RECORD_CONTENT:{col_name}::{data_type} as {col_name}"


def build_select_str(
    columns: list[str],
    primary_keys: list[str],
    column_types: Optional[dict[str, str]] = None
) -> str:
    """
    Build SELECT clause for all columns.

    Args:
        columns: List of column names
        primary_keys: List of primary key columns
        column_types: Optional dict mapping column_name -> data_type

    Returns:
        SELECT clause fragment
    """
    if column_types is None:
        column_types = {}

    selects = []
    for col in columns:
        col_clean = col.strip()
        data_type = column_types.get(col_clean)
        is_pk = any(pk.strip().upper() == col_clean.upper() for pk in primary_keys)

        selects.append(build_column_select(col_clean, data_type, is_pk))

    return ", ".join(selects)


def build_update_str(columns: list[str]) -> str:
    """
    Build UPDATE SET clause.

    Args:
        columns: List of column names

    Returns:
        UPDATE clause fragment, e.g., "a.col1 = b.col1, a.col2 = b.col2"
    """
    updates = []
    for col in columns:
        col = col.strip()
        updates.append(f"a.{col} = b.{col}")

    return ", ".join(updates)


def build_insert_str(columns: list[str]) -> str:
    """
    Build INSERT column list (value side).

    Args:
        columns: List of column names

    Returns:
        INSERT values fragment, e.g., "b.col1, b.col2, ..."
    """
    inserts = []
    for col in columns:
        col = col.strip()
        inserts.append(f"b.{col}")

    return ", ".join(inserts)


def build_delete_str(columns: list[str]) -> str:
    """
    Build DELETE filter condition.

    If 'chgts' column exists, use it as timestamp comparison.
    Otherwise, return '1=1' (all rows eligible for soft delete).

    Args:
        columns: List of column names

    Returns:
        WHERE clause condition, e.g., "a.chgts < b.record_created_date" or "1=1"
    """
    for col in columns:
        if col.strip().lower() == 'chgts':
            return "a.chgts < b.record_created_date"

    return "1=1"


def apply_debezium_mapping(
    select_str: str,
    update_str: str,
    unavailable_cols: list[str],
    primary_keys_part: str
) -> Tuple[str, str]:
    """
    Apply Debezium __debezium_unavailable_value handling.

    For columns where unavailable values exist, wrap the SELECT expression
    with FIRST_VALUE(NULLIF(...)) IGNORE NULLS window function and update
    UPDATE expressions with COALESCE fallback.

    Args:
        select_str: Original SELECT clause
        update_str: Original UPDATE SET clause
        unavailable_cols: List of column names with unavailable values
        primary_keys_part: Primary key partition expression

    Returns:
        Tuple of (mapped_select_str, mapped_update_str)
    """
    mapped_select = select_str
    mapped_update = update_str

    for col in unavailable_cols:
        col = col.strip()

        # Replace RECORD_CONTENT:col::TYPE as col with FIRST_VALUE window expression
        pattern = (
            rf"RECORD_CONTENT:{re.escape(col)}::\w+\s+as\s+{re.escape(col)}"
        )
        replacement = (
            f"FIRST_VALUE(NULLIF(RECORD_CONTENT:\"{col}\", '__debezium_unavailable_value')) "
            f"IGNORE NULLS OVER (PARTITION BY {primary_keys_part} "
            f"ORDER BY COALESCE(RECORD_METADATA:headers:__source_ts_ms, RECORD_CONTENT:__source_ts_ms) DESC, "
            f"COALESCE(RECORD_METADATA:headers:__lsn, RECORD_CONTENT:__lsn) DESC) AS {col}"
        )
        mapped_select = re.sub(pattern, replacement, mapped_select, flags=re.IGNORECASE)

        # Replace b.col with COALESCE(b.col, a.col) in update_str
        # Use lookahead to ensure we match column boundaries
        update_pattern = rf"b\.{re.escape(col)}(?=,|\s)"
        update_replacement = f"COALESCE(b.{col}, a.{col})"
        mapped_update = re.sub(update_pattern, update_replacement, mapped_update)

    return mapped_select, mapped_update


def build_temp_batch_sql(
    table_name: str,
    source_schema: str,
    primary_keys_part: str,
    select_str: str,
    landing_key_filter: int,
    tmp_schema: str = 'TMP'
) -> str:
    """
    Build CREATE TEMPORARY TABLE for batch processing.

    This temp table:
    - Extracts operation type (__op) from CDC metadata
    - Deduplicates by primary key (keeps latest by source timestamp/LSN)
    - Filters to new records since landing_key_filter

    Args:
        table_name: Name of landing table
        source_schema: Schema name (usually 'LANDING')
        primary_keys_part: PARTITION BY expression
        select_str: SELECT clause with typed columns
        landing_key_filter: Min landing_key to process (from last event)
        tmp_schema: Temp schema name

    Returns:
        Full CREATE TEMPORARY TABLE statement
    """
    sql = f"""
    CREATE OR REPLACE TEMPORARY TABLE {tmp_schema}.{table_name}_BATCH AS
    SELECT {table_name}_landing_key,
        COALESCE(RECORD_METADATA:headers:__op::STRING, RECORD_CONTENT:__op::STRING) AS operation,
        ROW_NUMBER() OVER (PARTITION BY {primary_keys_part}
                          ORDER BY COALESCE(RECORD_METADATA:headers:__source_ts_ms, RECORD_CONTENT:__source_ts_ms) DESC,
                                   COALESCE(RECORD_METADATA:headers:__lsn, RECORD_CONTENT:__lsn) DESC)
            AS RN_PRIMARY,
        ROW_NUMBER() OVER (PARTITION BY {primary_keys_part},
                          CASE WHEN operation <> 'd' THEN 'rcu' ELSE operation END
                          ORDER BY COALESCE(RECORD_METADATA:headers:__source_ts_ms, RECORD_CONTENT:__source_ts_ms) DESC,
                                   COALESCE(RECORD_METADATA:headers:__lsn, RECORD_CONTENT:__lsn) DESC)
            AS RN_PRIMARY_OP,
        TO_TIMESTAMP_NTZ(RECORD_METADATA:CreateTime::NUMBER/1000) AS record_created_date,
        {select_str}
    FROM {source_schema}.{table_name}
    WHERE {table_name}_landing_key > {landing_key_filter}
      AND LEN(TRIM(NVL(RECORD_CONTENT::STRING, 'X'))) >= 2
    QUALIFY RN_PRIMARY_OP = 1
    """

    return " ".join(sql.split())


def build_update_sql(
    table_name: str,
    target_schema: str,
    primary_keys_str: str,
    update_str: str,
    tmp_schema: str = 'TMP'
) -> str:
    """
    Build UPDATE statement for CDC records (non-deletes).

    Args:
        table_name: Table name
        target_schema: Target schema (e.g., 'IMPORT')
        primary_keys_str: Primary key join condition, e.g., "a.id = b.id"
        update_str: UPDATE SET clause
        tmp_schema: Temp schema name

    Returns:
        Full UPDATE statement
    """
    sql = f"""
    UPDATE {target_schema}.{table_name} a
    SET DW_UPDATED_DATE = CURRENT_TIMESTAMP(),
        DW_DELETED_DATE = NULL,
        IS_DELETED = FALSE,
        RECORD_CHANGE_FLAG = b.operation,
        {update_str}
    FROM {tmp_schema}.{table_name}_BATCH b
    WHERE {primary_keys_str}
      AND b.operation <> 'd'
      AND b.RN_PRIMARY = 1
    """

    return " ".join(sql.split())


def build_insert_sql(
    table_name: str,
    target_schema: str,
    primary_keys_str: str,
    insert_str: str,
    tmp_schema: str = 'TMP'
) -> str:
    """
    Build INSERT statement for new CDC records.

    Args:
        table_name: Table name
        target_schema: Target schema
        primary_keys_str: Primary key join condition
        insert_str: INSERT columns (b.col1, b.col2, ...)
        tmp_schema: Temp schema name

    Returns:
        Full INSERT statement
    """
    # Extract column names from insert_str (e.g., "b.col1, b.col2" -> "col1, col2")
    insert_cols = ", ".join(
        col.split(".")[-1] for col in insert_str.split(", ")
    )

    sql = f"""
    INSERT INTO {target_schema}.{table_name} ({insert_cols}, DW_CREATED_DATE, DW_UPDATED_DATE, DW_DELETED_DATE, IS_DELETED, RECORD_CHANGE_FLAG)
    SELECT {insert_str}, CURRENT_TIMESTAMP(), CASE WHEN b.operation = 'u' THEN CURRENT_TIMESTAMP() ELSE NULL END, NULL, FALSE, b.operation
    FROM {tmp_schema}.{table_name}_BATCH b
    WHERE b.operation <> 'd'
      AND NOT EXISTS (
        SELECT 1 FROM {target_schema}.{table_name} a
        WHERE {primary_keys_str}
      )
    """

    return " ".join(sql.split())


def build_delete_sql(
    table_name: str,
    target_schema: str,
    primary_keys_str: str,
    delete_condition: str,
    tmp_schema: str = 'TMP'
) -> str:
    """
    Build soft-DELETE statement for CDC delete records.

    Args:
        table_name: Table name
        target_schema: Target schema
        primary_keys_str: Primary key join condition
        delete_condition: DELETE filter, e.g., "a.chgts < b.record_created_date"
        tmp_schema: Temp schema name

    Returns:
        Full UPDATE statement (soft delete)
    """
    sql = f"""
    UPDATE {target_schema}.{table_name} a
    SET DW_DELETED_DATE = b.record_created_date,
        IS_DELETED = TRUE,
        RECORD_CHANGE_FLAG = b.operation
    FROM {tmp_schema}.{table_name}_BATCH b
    WHERE {primary_keys_str}
      AND IS_DELETED = FALSE
      AND b.operation = 'd'
      AND b.RN_PRIMARY = 1
      AND {delete_condition}
    """

    return " ".join(sql.split())


def build_column_check_sql(
    table_name: str,
    source_schema: str = 'LANDING'
) -> str:
    """
    Build SQL to check for new columns in landing table.

    Returns a CTE query that identifies all columns in RECORD_CONTENT
    and their inferred types.

    Args:
        table_name: Landing table name
        source_schema: Landing schema name

    Returns:
        SQL string for column detection
    """
    sql = f"""
    WITH column_samples AS (
        SELECT
            f.value::STRING AS COLUMN_NAME,
            MAX(RECORD_CONTENT[f.value::STRING]) as sample_value
        FROM {source_schema}.{table_name},
        LATERAL FLATTEN(input => OBJECT_KEYS(RECORD_CONTENT)) f
        WHERE DATE(DW_CREATED_DATE) >= DATEADD(DAY, -5, CURRENT_DATE())
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
        AND c.table_schema = (SELECT target_schema FROM (VALUES ('IMPORT')) AS t(target_schema))
    ORDER BY cs.COLUMN_NAME
    """

    return " ".join(sql.split())
