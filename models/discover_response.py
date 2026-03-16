"""DiscoverResponse — schema for discover endpoint response."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel


class ColumnInfo(BaseModel):
    """Column metadata from discovery."""

    name: str
    type: str
    nullable: bool = True
    primary_key: bool = False
    default: str | None = None


class TableInfo(BaseModel):
    """Table metadata from discovery."""

    name: str
    row_count_estimate: int = 0
    columns: list[ColumnInfo]
    primary_keys: list[str] = []
    indexes: list[str] = []
    replication_key_candidates: list[str] = []


class SchemaInfo(BaseModel):
    """Schema with tables."""

    name: str
    tables: list[dict[str, Any]]


class DiscoverResponse(BaseModel):
    """Discover endpoint response structure."""

    schemas: list[dict[str, Any]]
