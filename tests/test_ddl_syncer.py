"""Unit tests for DDL syncer (mocked Snowflake connection).

These tests mock the Snowflake cursor to verify correct SQL execution sequence
without needing an actual Snowflake connection.
"""

import unittest
from unittest.mock import Mock, MagicMock, patch, call


class TestDDLSyncerTable(unittest.TestCase):
    """Tests for sync_table function."""

    @patch('nano_chop.ddl_syncer.get_connection')
    def test_sync_table_no_landing_data(self, mock_get_conn):
        """Test sync_table when landing table is empty."""
        from nano_chop.ddl_syncer import sync_table

        # Mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # First query: no landing data
        mock_cursor.fetchone.return_value = (0,)

        result = sync_table(
            mock_conn,
            'TEST_TABLE',
            'IMPORT',
            run_mode=''
        )

        # Should return early with no-data message
        self.assertIn('No records available', result[0])
        # Execute should only be called once (the count query)
        self.assertEqual(mock_cursor.execute.call_count, 1)

    @patch('nano_chop.ddl_syncer.get_connection')
    def test_sync_table_first_time_with_pks(self, mock_get_conn):
        """Test sync_table when table is new (first_time=True)."""
        from nano_chop.ddl_syncer import sync_table

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Setup mock return values
        mock_cursor.fetchone.side_effect = [
            (100,),  # Landing count check: has data
            None,    # Control table check: no entry
            (['id', 'name'],),  # Get primary keys
            (('col1', 'VARCHAR', None),),  # Column detection
        ]
        mock_cursor.fetchall.return_value = [
            ('col1', 'VARCHAR', None),
            ('col2', 'INT', None),
        ]

        result = sync_table(
            mock_conn,
            'TEST_TABLE',
            'IMPORT',
            run_mode='TEST'
        )

        # Should initialize control entry
        self.assertTrue(any('Initialized ddl_control' in r for r in result))
        # Should commit after insert
        self.assertTrue(mock_conn.commit.called)

    @patch('nano_chop.ddl_syncer.get_connection')
    def test_sync_table_new_columns_detected(self, mock_get_conn):
        """Test sync_table when new columns are found."""
        from nano_chop.ddl_syncer import sync_table

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_cursor.fetchone.side_effect = [
            (100,),  # Landing count
            (1,),    # Control exists
        ]
        mock_cursor.fetchall.return_value = [
            ('id', 'INT', 'INT'),  # Existing
            ('new_col', 'VARCHAR', None),  # New column
        ]

        result = sync_table(
            mock_conn,
            'TEST_TABLE',
            'IMPORT',
            run_mode='TEST'
        )

        # Should detect new columns
        self.assertTrue(any('new columns' in r.lower() for r in result))
        # ALTER TABLE should be called
        alter_calls = [
            str(call_args)
            for call_args in mock_cursor.execute.call_args_list
            if 'ALTER TABLE' in str(call_args)
        ]
        self.assertTrue(len(alter_calls) > 0)


class TestDDLSyncerAllTables(unittest.TestCase):
    """Tests for sync_all_tables function."""

    def test_sync_all_tables_filters_by_groups(self):
        """Test that sync_all_tables respects group filtering."""
        from nano_chop.ddl_syncer import sync_all_tables

        config = {
            'environments': {
                'test': {
                    'account': 'test.snowflakecomputing.com',
                    'user': 'TEST',
                    'warehouse': 'WH',
                    'database': 'DB',
                    'private_key_path': '/path/key',
                }
            },
            'tables': {
                'group1': ['table1', 'table2'],
                'group2': ['table3', 'table4'],
            },
        }

        with patch('nano_chop.ddl_syncer.get_connection') as mock_get_conn:
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn

            with patch('nano_chop.ddl_syncer.sync_table') as mock_sync:
                mock_sync.return_value = ['OK']

                # Sync only group1
                result = sync_all_tables(
                    config,
                    env='test',
                    groups=['group1'],
                    max_workers=2
                )

                # Should sync only table1 and table2
                synced_tables = [call_args[0][2] for call_args in mock_sync.call_args_list]
                self.assertIn('table1', synced_tables)
                self.assertIn('table2', synced_tables)
                self.assertNotIn('table3', synced_tables)


if __name__ == '__main__':
    unittest.main()
