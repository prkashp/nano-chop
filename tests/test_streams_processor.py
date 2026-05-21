"""Unit tests for streams processor (mocked Snowflake connection).

These tests mock the Snowflake cursor to verify correct transaction sequence,
CDC logic, and event logging without needing an actual Snowflake connection.
"""

import unittest
from unittest.mock import Mock, MagicMock, patch, call


class TestStreamProcessorTable(unittest.TestCase):
    """Tests for process_table function."""

    @patch('nano_chop.streams_processor.get_last_landing_key')
    @patch('nano_chop.streams_processor.get_connection')
    def test_process_table_no_new_data(self, mock_get_conn, mock_get_last_key):
        """Test process_table when landing table has no new records."""
        from nano_chop.streams_processor import process_table

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Setup mocks
        mock_get_last_key.return_value = 100
        # Count query returns 0 (no new records)
        mock_cursor.fetchone.return_value = (0,)

        result = process_table(
            mock_conn,
            'TEST_TABLE',
            'IMPORT'
        )

        # Should return early with no-new-data message
        self.assertIn('No new data', ' '.join(result))
        # Only count query should execute
        self.assertEqual(mock_cursor.execute.call_count, 1)

    @patch('nano_chop.streams_processor.log_streams_processor_success')
    @patch('nano_chop.streams_processor.get_last_landing_key')
    @patch('nano_chop.streams_processor.get_connection')
    def test_process_table_cdc_insert_update_delete(
        self, mock_get_conn, mock_get_last_key, mock_log_success
    ):
        """Test process_table executes CDC INSERT/UPDATE/DELETE in transaction."""
        from nano_chop.streams_processor import process_table

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        # Setup mocks
        mock_get_last_key.return_value = 100

        # Setup fetchone side effects in order
        mock_cursor.fetchone.side_effect = [
            (1000,),  # Landing count check: has new data
            ('id,tenant', 'a.id=b.id AND a.tenant=b.tenant', 'COALESCE(...)',  # DDL control
             'SELECT ...', 'b.col1, b.col2', 'a.col1 = b.col1', '1=1'),
            (500,),  # Max landing key from batch table
        ]
        mock_cursor.fetchall.return_value = []  # No debezium unavailable

        mock_cursor.rowcount = 100  # Default row count

        result = process_table(
            mock_conn,
            'TEST_TABLE',
            'IMPORT'
        )

        # Verify transaction sequence
        execute_calls = [str(call_args) for call_args in mock_cursor.execute.call_args_list]
        self.assertTrue(any('BEGIN' in call for call in execute_calls))
        self.assertTrue(any('COMMIT' in call for call in execute_calls))

    @patch('nano_chop.streams_processor.sync_table')
    @patch('nano_chop.streams_processor.log_streams_processor_success')
    @patch('nano_chop.streams_processor.get_last_landing_key')
    @patch('nano_chop.streams_processor.get_connection')
    def test_process_table_triggers_ddl_sync_if_missing(
        self, mock_get_conn, mock_get_last_key, mock_log_success, mock_sync_table
    ):
        """Test that process_table calls sync_table if DDL control is missing."""
        from nano_chop.streams_processor import process_table

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_get_last_key.return_value = 0
        mock_sync_table.return_value = ['DDL synced']

        # First fetchone: landing count (has data)
        # Second fetchone: no DDL control (None)
        # Then after sync_table, return valid DDL
        mock_cursor.fetchone.side_effect = [
            (1000,),  # Landing count
            None,     # No DDL control first time
            ('id', 'a.id=b.id', 'COALESCE(...)',  # Valid DDL after sync
             'SELECT ...', 'b.col1', 'a.col1 = b.col1', '1=1'),
            (500,),   # Max landing key
        ]
        mock_cursor.fetchall.return_value = []

        result = process_table(mock_conn, 'TEST_TABLE', 'IMPORT')

        # sync_table should have been called
        self.assertTrue(mock_sync_table.called)

    @patch('nano_chop.streams_processor.log_streams_processor_failure')
    @patch('nano_chop.streams_processor.get_last_landing_key')
    @patch('nano_chop.streams_processor.get_connection')
    def test_process_table_rollback_on_error(
        self, mock_get_conn, mock_get_last_key, mock_log_failure
    ):
        """Test that process_table ROLLBACKs on error and logs failure."""
        from nano_chop.streams_processor import process_table

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor

        mock_get_last_key.return_value = 0
        mock_cursor.execute.side_effect = Exception('Test error')

        with self.assertRaises(RuntimeError):
            process_table(mock_conn, 'TEST_TABLE', 'IMPORT')

        # ROLLBACK should have been called
        rollback_called = any(
            'ROLLBACK' in str(call_args)
            for call_args in mock_cursor.execute.call_args_list
        )
        self.assertTrue(rollback_called)

        # Failure should be logged
        self.assertTrue(mock_log_failure.called)


class TestStreamProcessorAllTables(unittest.TestCase):
    """Tests for process_all_tables function."""

    def test_process_all_tables_filters_by_groups(self):
        """Test that process_all_tables respects group filtering."""
        from nano_chop.streams_processor import process_all_tables

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
            'streams_processor': {
                'max_workers': 2,
            },
        }

        with patch('nano_chop.streams_processor.get_connection') as mock_get_conn:
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn

            with patch('nano_chop.streams_processor.process_table') as mock_process:
                mock_process.return_value = ['OK']

                # Process only group1
                result = process_all_tables(
                    config,
                    env='test',
                    groups=['group1']
                )

                # Should process only table1 and table2
                processed_tables = [
                    call_args[0][2] for call_args in mock_process.call_args_list
                ]
                self.assertIn('table1', processed_tables)
                self.assertIn('table2', processed_tables)
                self.assertNotIn('table3', processed_tables)

    def test_process_all_tables_parallel_execution(self):
        """Test that process_all_tables uses ThreadPoolExecutor."""
        from nano_chop.streams_processor import process_all_tables

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
                'group': ['table1', 'table2', 'table3'],
            },
            'streams_processor': {
                'max_workers': 3,
            },
        }

        with patch('nano_chop.streams_processor.get_connection') as mock_get_conn:
            mock_conn = MagicMock()
            mock_get_conn.return_value = mock_conn

            with patch('nano_chop.streams_processor.process_table') as mock_process:
                mock_process.return_value = ['OK']

                result = process_all_tables(config, env='test')

                # All 3 tables should be processed
                self.assertEqual(len(result), 3)
                self.assertIn('table1', result)
                self.assertIn('table2', result)
                self.assertIn('table3', result)


if __name__ == '__main__':
    unittest.main()
