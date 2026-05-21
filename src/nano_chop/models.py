"""Data models for nano-chop."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class DDLControlRecord:
    """Represents a row from ADMIN.DDL_CONTROL table."""

    table_name: str
    primary_keys: str
    primary_keys_str: str
    primary_keys_part: str
    select_str: str
    insert_str: str
    update_str: str
    delete_str: str
    created_at: Optional[str] = None
    created_by: Optional[str] = None
    updated_at: Optional[str] = None
    updated_by: Optional[str] = None

    @classmethod
    def from_cursor_row(cls, row: tuple) -> 'DDLControlRecord':
        """Create instance from cursor fetchone() result."""
        return cls(
            table_name=row[0],
            primary_keys=row[1],
            primary_keys_str=row[2],
            primary_keys_part=row[3],
            select_str=row[4],
            insert_str=row[5],
            update_str=row[6],
            delete_str=row[7],
            created_at=row[8] if len(row) > 8 else None,
            created_by=row[9] if len(row) > 9 else None,
            updated_at=row[10] if len(row) > 10 else None,
            updated_by=row[11] if len(row) > 11 else None,
        )


@dataclass
class ColumnDef:
    """Column definition with type information."""

    name: str
    landing_type: Optional[str] = None
    target_type: Optional[str] = None
    is_primary_key: bool = False
