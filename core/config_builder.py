"""Build tap-postgres and target-postgres config JSON dicts from connection credentials.

Config key mapping (NestJS -> tap/target):
- username -> user (both accepted)
- database / dbname -> database (both accepted)
- host / hostname -> host (both accepted)

Supabase host formats:
- Direct: db.<project-ref>.supabase.co
- Pooler: aws-0-<region>.pooler.supabase.com (port 5432 session, 6543 transaction)
- Common typo: b.<ref>.supabase.co -> auto-corrected to db.<ref>.supabase.co
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import quote_plus

logger = logging.getLogger("etl.config")


def _normalize_supabase_host(host: str) -> str:
    """Fix common Supabase host typo: b. -> db. for *.supabase.co hosts."""
    if not host or not isinstance(host, str):
        return host
    # b.<project-ref>.supabase.co is invalid; correct to db.<project-ref>.supabase.co
    if host.startswith("b.") and ".supabase.co" in host:
        fixed = "db." + host[2:]
        logger.info("Normalized Supabase host: %s -> %s", host, fixed)
        return fixed
    return host


def _build_sqlalchemy_url(conn: dict[str, Any]) -> str:
    """Build postgresql+psycopg URL with sslmode=require (encrypt without cert verification).

    Used for tap-postgres and target-postgres to bypass their connector's SSL logic,
    which sets sslrootcert when ssl_enable=True and causes 'certificate verify failed'
    with cloud Postgres poolers.
    """
    raw_host = conn.get("host") or conn.get("hostname", "localhost")
    host = _normalize_supabase_host(raw_host)
    port = int(conn.get("port", 5432))
    user = conn.get("user") or conn.get("username", "postgres")
    password = conn.get("password", "")
    database = conn.get("database") or conn.get("dbname", "postgres")
    encoded_password = quote_plus(str(password))
    return f"postgresql+psycopg://{user}:{encoded_password}@{host}:{port}/{database}?sslmode=require"


def _extract_common(conn: dict[str, Any]) -> dict[str, Any]:
    """Extract common PG connection fields from the NestJS decrypted config.

    SSL is determined solely from the stored ssl field (bool, dict, or string).
    """
    raw_host = conn.get("host") or conn.get("hostname", "localhost")
    host = _normalize_supabase_host(raw_host)
    port = int(conn.get("port", 5432))
    user = conn.get("user") or conn.get("username", "postgres")
    password = conn.get("password", "")
    database = conn.get("database") or conn.get("dbname", "postgres")

    config: dict[str, Any] = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
    }

    # SSL detection: from stored config only (ssl field)
    ssl = conn.get("ssl")
    needs_ssl = False
    if isinstance(ssl, bool):
        needs_ssl = ssl
    elif isinstance(ssl, str) and str(ssl).lower() in ("true", "1", "yes"):
        needs_ssl = True
    elif isinstance(ssl, dict):
        needs_ssl = bool(
            ssl.get("enabled")
            or ssl.get("ca_cert")
            or ssl.get("client_cert")
            or ssl.get("client_key")
            or ssl.get("require")
        )

    if needs_ssl:
        config["ssl_enable"] = True
        config["ssl_mode"] = "require"

    logger.info(
        "Built config: host=%s port=%d user=%s database=%s ssl=%s (input keys: %s)",
        host, port, user, database, needs_ssl, sorted(conn.keys()),
    )

    return config


def build_tap_config(
    conn: dict[str, Any],
    replication_slot_name: str | None = None,
) -> dict[str, Any]:
    """Build config dict for tap-postgres CLI.

    When SSL is needed, uses sqlalchemy_url exclusively (removes individual params)
    so the tap uses the URL with sslmode=require.
    """
    config = _extract_common(conn)
    if config.get("ssl_enable"):
        config["sqlalchemy_url"] = _build_sqlalchemy_url(conn)
        # Remove individual params so tap uses sqlalchemy_url exclusively
        for key in ("host", "port", "user", "password", "database", "ssl_enable", "ssl_mode"):
            config.pop(key, None)

    if replication_slot_name:
        config["replication_slot_name"] = replication_slot_name

    return config


def build_target_config(
    conn: dict[str, Any],
    dest_schema: str = "public",
    *,
    hard_delete: bool = False,
    source_stream: str | None = None,
    dest_table: str | None = None,
    emit_method: str = "append",
    upsert_key: list[str] | None = None,
) -> dict[str, Any]:
    """Build config dict for target-postgres CLI.

    When source_stream and dest_table are provided and differ, a stream_maps
    entry with ``__alias__`` is added so target-postgres writes to the
    user-selected destination table instead of the Singer stream name.

    When SSL is needed, uses sqlalchemy_url exclusively (removes individual params)
    so the target uses the URL with sslmode=require.
    """
    config = _extract_common(conn)
    if config.get("ssl_enable"):
        config["sqlalchemy_url"] = _build_sqlalchemy_url(conn)
        # Remove individual params so target uses sqlalchemy_url exclusively
        for key in ("host", "port", "user", "password", "database", "ssl_enable", "ssl_mode"):
            config.pop(key, None)
    config["default_target_schema"] = dest_schema
    config["add_record_metadata"] = True
    config["activate_version"] = True
    config["hard_delete"] = hard_delete
    config["emit_method"] = emit_method
    if upsert_key:
        config["upsert_key"] = upsert_key

    if source_stream and dest_table and source_stream != dest_table:
        config["stream_maps"] = {
            source_stream: {"__alias__": dest_table}
        }
        logger.info(
            "stream_maps: '%s' -> '%s'", source_stream, dest_table,
        )

    return config
