"""Shared SQL connector normalization for ETL routes and builders."""

from __future__ import annotations

SUPPORTED_SQL_SOURCE_TYPES = {
    "postgres",
    "mysql",
    "mariadb",
    "mssql",
    "oracle",
    "sqlite",
    "cockroachdb",
}

SUPPORTED_SQL_DEST_TYPES = set(SUPPORTED_SQL_SOURCE_TYPES)

SOURCE_TYPE_ALIASES = {
    "postgresql": "postgres",
    "pg": "postgres",
    "sqlserver": "mssql",
}

DEST_TYPE_ALIASES = {
    "postgresql": "postgres",
    "pg": "postgres",
    "sqlserver": "mssql",
}


def _normalize_raw(value: str | None, default: str) -> str:
    normalized = (value or default).strip().lower()
    return normalized or default


def normalize_source_type(source_type: str | None = None) -> str:
    """Normalize source connector aliases to the canonical SQL registry key."""
    normalized = SOURCE_TYPE_ALIASES.get(_normalize_raw(source_type, "postgres")) or _normalize_raw(
        source_type,
        "postgres",
    )
    if normalized not in SUPPORTED_SQL_SOURCE_TYPES:
        raise ValueError(
            f"Unsupported source connector type '{normalized}'. Supported SQL sources: "
            f"{', '.join(sorted(SUPPORTED_SQL_SOURCE_TYPES))}."
        )
    return normalized


def normalize_dest_type(dest_type: str | None = None) -> str:
    """Normalize destination connector aliases to the canonical SQL registry key."""
    normalized = DEST_TYPE_ALIASES.get(_normalize_raw(dest_type, "postgres")) or _normalize_raw(
        dest_type,
        "postgres",
    )
    if normalized not in SUPPORTED_SQL_DEST_TYPES:
        raise ValueError(
            f"Unsupported destination connector type '{normalized}'. Supported SQL destinations: "
            f"{', '.join(sorted(SUPPORTED_SQL_DEST_TYPES))}."
        )
    return normalized
