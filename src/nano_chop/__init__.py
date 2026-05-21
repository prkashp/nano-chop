"""nano-chop: Python CDC Pipeline for Snowflake"""

__version__ = "0.1.0"
__author__ = "Prakash Pandey"

from .ddl_syncer import sync_table, sync_all_tables
from .streams_processor import process_table, process_all_tables
from .connection import get_connection, ConnectionPool

__all__ = [
    "sync_table",
    "sync_all_tables",
    "process_table",
    "process_all_tables",
    "get_connection",
    "ConnectionPool",
]
