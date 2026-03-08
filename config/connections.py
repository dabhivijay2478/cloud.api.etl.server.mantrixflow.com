"""Shared connection helpers — delegates to core.config_builder.

All connection resolution (SSL, URL encoding, pooler detection, timeouts)
lives in core.config_builder. This module is a thin compatibility shim so
existing callers of build_postgres_conn_str keep working without changes.
"""

from __future__ import annotations

from core.config_builder import build_psycopg_dsn


def build_postgres_conn_str(config: dict) -> str:
    """Return a libpq DSN for the given connection config.

    Delegates entirely to core.config_builder.build_psycopg_dsn so that
    SSL mode, URL encoding, pooler flags, and connect_timeout are resolved
    identically to every other connection in the system.
    """
    return build_psycopg_dsn(config)


def build_mongo_conn_url(config: dict) -> str:  # noqa: ARG001
    """Placeholder — MongoDB support removed."""
    raise NotImplementedError("MongoDB is not supported in this ETL server.")


def parse_stream(stream: str) -> tuple[str, str]:
    """Parse 'schema-table' or 'schema.table' into (schema, table).

    Examples
    --------
    >>> parse_stream("public-orders")
    ('public', 'orders')
    >>> parse_stream("public.orders")
    ('public', 'orders')
    """
    for sep in ("-", "."):
        if sep in stream:
            parts = stream.split(sep, 1)
            return parts[0], parts[1]
    return "public", stream
