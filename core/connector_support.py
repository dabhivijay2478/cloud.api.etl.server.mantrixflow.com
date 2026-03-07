"""Shared connector type normalization for ETL routes and builders."""

from __future__ import annotations

SOURCE_TYPE_ALIASES = {
    "postgresql": "postgres",
    "pg": "postgres",
    "pgvector": "postgres",
    "redshift": "postgres",
    "source-postgres": "postgres",
    "source-mongodb-v2": "mongodb",
    "sqlserver": "mssql",
    "source-mssql": "mssql",
}

DEST_TYPE_ALIASES = {
    "postgresql": "postgres",
    "pg": "postgres",
    "pgvector": "postgres",
    "redshift": "postgres",
    "target-postgres": "postgres",
    "destination-postgres": "postgres",
    "sqlserver": "mssql",
}


def _normalize_raw(value: str | None, default: str) -> str:
    normalized = (value or default).strip().lower()
    return normalized or default


def _strip_prefixes(value: str) -> str:
    normalized = value
    for prefix in ("source-", "target-", "destination-"):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix) :]
    return normalized


def normalize_source_type(source_type: str | None = None) -> str:
    """Normalize source connector aliases to the tap registry key."""
    normalized = _normalize_raw(source_type, "postgres")
    stripped = _strip_prefixes(normalized)
    return SOURCE_TYPE_ALIASES.get(normalized) or SOURCE_TYPE_ALIASES.get(stripped) or stripped


def normalize_dest_type(dest_type: str | None = None) -> str:
    """Normalize destination connector aliases to the target registry key."""
    normalized = _normalize_raw(dest_type, "postgres")
    stripped = _strip_prefixes(normalized)
    return DEST_TYPE_ALIASES.get(normalized) or DEST_TYPE_ALIASES.get(stripped) or stripped
