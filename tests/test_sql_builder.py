"""Unit tests for SQL builder functions (no Snowflake connection required)."""

import unittest
from nano_chop.sql_builder import (
    build_pk_join_condition,
    build_pk_partition_expr,
    build_column_select,
    build_select_str,
    build_update_str,
    build_insert_str,
    build_delete_str,
    apply_debezium_mapping,
)


class TestPKJoinCondition(unittest.TestCase):
    """Tests for build_pk_join_condition."""

    def test_single_pk(self):
        result = build_pk_join_condition(['id'])
        self.assertEqual(result, 'a.id = b.id')

    def test_multiple_pks(self):
        result = build_pk_join_condition(['id', 'tenant_id'])
        self.assertIn('a.id = b.id', result)
        self.assertIn('a.tenant_id = b.tenant_id', result)
        self.assertIn('AND', result)

    def test_empty_pks(self):
        result = build_pk_join_condition([])
        self.assertEqual(result, '1=1')

    def test_pks_with_whitespace(self):
        result = build_pk_join_condition([' id ', ' tenant_id '])
        self.assertIn('a.id = b.id', result)
        self.assertIn('a.tenant_id = b.tenant_id', result)


class TestPKPartitionExpr(unittest.TestCase):
    """Tests for build_pk_partition_expr."""

    def test_single_pk(self):
        result = build_pk_partition_expr(['id'])
        self.assertIn('COALESCE(RECORD_METADATA:key:id', result)
        self.assertIn('RECORD_CONTENT:id)', result)

    def test_multiple_pks(self):
        result = build_pk_partition_expr(['id', 'tenant_id'])
        self.assertIn('COALESCE(RECORD_METADATA:key:id', result)
        self.assertIn('COALESCE(RECORD_METADATA:key:tenant_id', result)
        self.assertIn(',', result)

    def test_empty_pks(self):
        result = build_pk_partition_expr([])
        self.assertEqual(result, '1')


class TestColumnSelect(unittest.TestCase):
    """Tests for build_column_select."""

    def test_non_pk_column(self):
        result = build_column_select('name', 'VARCHAR', is_primary_key=False)
        self.assertEqual(result, 'RECORD_CONTENT:name::VARCHAR as name')

    def test_pk_column(self):
        result = build_column_select('id', 'INT', is_primary_key=True)
        self.assertIn('COALESCE(RECORD_METADATA:key:id::INT', result)
        self.assertIn('RECORD_CONTENT:id::INT)', result)

    def test_none_type_defaults_to_varchar(self):
        result = build_column_select('col', None, is_primary_key=False)
        self.assertEqual(result, 'RECORD_CONTENT:col::VARCHAR as col')


class TestUpdateStr(unittest.TestCase):
    """Tests for build_update_str."""

    def test_single_column(self):
        result = build_update_str(['name'])
        self.assertEqual(result, 'a.name = b.name')

    def test_multiple_columns(self):
        result = build_update_str(['name', 'email', 'status'])
        self.assertIn('a.name = b.name', result)
        self.assertIn('a.email = b.email', result)
        self.assertIn('a.status = b.status', result)


class TestInsertStr(unittest.TestCase):
    """Tests for build_insert_str."""

    def test_single_column(self):
        result = build_insert_str(['name'])
        self.assertEqual(result, 'b.name')

    def test_multiple_columns(self):
        result = build_insert_str(['id', 'name', 'email'])
        self.assertEqual(result, 'b.id, b.name, b.email')


class TestDeleteStr(unittest.TestCase):
    """Tests for build_delete_str."""

    def test_with_chgts_column(self):
        result = build_delete_str(['id', 'chgts', 'name'])
        self.assertEqual(result, 'a.chgts < b.record_created_date')

    def test_without_chgts_column(self):
        result = build_delete_str(['id', 'name', 'email'])
        self.assertEqual(result, '1=1')

    def test_empty_columns(self):
        result = build_delete_str([])
        self.assertEqual(result, '1=1')


class TestDebeziumMapping(unittest.TestCase):
    """Tests for apply_debezium_mapping."""

    def test_no_unavailable_columns(self):
        select = 'RECORD_CONTENT:col1::VARCHAR as col1'
        update = 'a.col1 = b.col1'

        mapped_select, mapped_update = apply_debezium_mapping(
            select, update, [], 'COALESCE(RECORD_METADATA:key:id, RECORD_CONTENT:id)'
        )

        # Should be unchanged
        self.assertEqual(mapped_select, select)
        self.assertEqual(mapped_update, update)

    def test_single_unavailable_column(self):
        select = 'RECORD_CONTENT:col1::VARCHAR as col1, RECORD_CONTENT:col2::VARCHAR as col2'
        update = 'a.col1 = b.col1, a.col2 = b.col2'

        mapped_select, mapped_update = apply_debezium_mapping(
            select, update, ['col1'], 'COALESCE(RECORD_METADATA:key:id, RECORD_CONTENT:id)'
        )

        # col1 should be mapped to FIRST_VALUE, col2 should be unchanged
        self.assertIn('FIRST_VALUE', mapped_select)
        self.assertIn('col2::VARCHAR as col2', mapped_select)
        self.assertIn('COALESCE(b.col1, a.col1)', mapped_update)
        self.assertIn('a.col2 = b.col2', mapped_update)


if __name__ == '__main__':
    unittest.main()
