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

import psycopg2
from core.connector_support import normalize_dest_type, normalize_source_type

logger = logging.getLogger("etl.config")

# Fast connection test timeout (seconds) — avoids tap-postgres subprocess overhead
TEST_CONNECTION_TIMEOUT_SEC = 10


def _build_sqlalchemy_url(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    ssl_mode: str | None = None,
) -> str:
    """Build PostgreSQL SQLAlchemy URL for tap/target config.

    Uses quote_plus for user and password to handle special characters.
    """
    user_enc = quote_plus(user)
    password_enc = quote_plus(password)
    url = f"postgresql://{user_enc}:{password_enc}@{host}:{port}/{database}"
    if ssl_mode:
        url += f"?sslmode={quote_plus(ssl_mode)}"
    return url


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

    # sqlalchemy_url is the primary connection method (tap-postgres recommended).
    # Bypasses CLI callback ordering and config key filtering issues.
    ssl_mode = "require" if needs_ssl else None
    config["sqlalchemy_url"] = _build_sqlalchemy_url(
        host, port, user, password, database, ssl_mode
    )

    logger.info(
        "Built config: host=%s port=%d user=%s database=%s ssl=%s (input keys: %s)",
        host, port, user, database, needs_ssl, sorted(conn.keys()),
    )

    return config


def _build_postgres_tap_config(
    conn: dict[str, Any],
    replication_slot_name: str | None = None,
) -> dict[str, Any]:
    """Build config dict for tap-postgres CLI.

    Uses sqlalchemy_url as primary connection method (tap-postgres recommended).
    Also includes host, port, user, password, database as fallback.
    """
    config = _extract_common(conn)

    if replication_slot_name:
        config["replication_slot_name"] = replication_slot_name

    return config


def _build_postgres_target_config(
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

    Uses sqlalchemy_url as primary connection method (same as tap-postgres).
    """
    config = _extract_common(conn)
    config["default_target_schema"] = dest_schema
    config["add_record_metadata"] = True
    config["activate_version"] = True
    config["hard_delete"] = hard_delete
    config["load_method"] = _map_emit_method_to_load_method(emit_method)

    if source_stream and dest_table and source_stream != dest_table:
        config["stream_maps"] = {
            source_stream: {"__alias__": dest_table}
        }
        logger.info(
            "stream_maps: '%s' -> '%s'", source_stream, dest_table,
        )

    return config


def _map_emit_method_to_load_method(emit_method: str) -> str:
    """Translate app-level write mode into target-postgres load_method."""
    normalized = (emit_method or "append").strip().lower()
    if normalized == "upsert":
        return "upsert"
    if normalized in ("replace", "overwrite"):
        return "overwrite"
    return "append-only"


def _test_postgres_connection_fast(conn: dict[str, Any]) -> dict[str, Any]:
    """Test PostgreSQL connectivity with psycopg2 (SELECT 1). Completes in <2s typically.

    Avoids tap-postgres subprocess which does full discovery and can exceed 60s for
    cloud DBs (Neon cold start, network latency). Use for /test-connection endpoint.
    """
    cfg = _extract_common(conn)
    host = cfg["host"]
    port = cfg["port"]
    user = cfg["user"]
    password = cfg["password"]
    database = cfg["database"]
    needs_ssl = cfg.get("ssl_enable", False)
    sslmode = "require" if needs_ssl else "prefer"

    try:
        with psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            dbname=database,
            connect_timeout=TEST_CONNECTION_TIMEOUT_SEC,
            sslmode=sslmode,
        ) as pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"success": True}
    except Exception as e:
        err = str(e).strip()
        if not err:
            err = type(e).__name__
        logger.warning("Fast connection test failed for %s:%s/%s: %s", host, port, database, err)
        return {"success": False, "error": err}


TAP_CONFIG_BUILDERS = {
    "postgres": _build_postgres_tap_config,
}

TARGET_CONFIG_BUILDERS = {
    "postgres": _build_postgres_target_config,
}

CONNECTION_TESTERS = {
    "postgres": _test_postgres_connection_fast,
}


def resolve_tap_type(source_type: str | None = None) -> str:
    """Normalize and validate a source type against registered tap builders."""
    normalized = normalize_source_type(source_type)
    if normalized not in TAP_CONFIG_BUILDERS:
        supported = ", ".join(sorted(TAP_CONFIG_BUILDERS))
        raise ValueError(
            f"Unsupported source_type={source_type!r}. Registered tap builders: {supported}"
        )
    return normalized


def resolve_target_type(dest_type: str | None = None) -> str:
    """Normalize and validate a destination type against registered target builders."""
    normalized = normalize_dest_type(dest_type)
    if normalized not in TARGET_CONFIG_BUILDERS:
        supported = ", ".join(sorted(TARGET_CONFIG_BUILDERS))
        raise ValueError(
            f"Unsupported dest_type={dest_type!r}. Registered target builders: {supported}"
        )
    return normalized


def build_tap_config(
    conn: dict[str, Any],
    replication_slot_name: str | None = None,
    *,
    source_type: str = "postgres",
) -> dict[str, Any]:
    """Build the tap config for the requested registered source type."""
    normalized = resolve_tap_type(source_type)
    builder = TAP_CONFIG_BUILDERS[normalized]
    return builder(conn, replication_slot_name)


def build_target_config(
    conn: dict[str, Any],
    dest_schema: str = "public",
    *,
    hard_delete: bool = False,
    source_stream: str | None = None,
    dest_table: str | None = None,
    emit_method: str = "append",
    upsert_key: list[str] | None = None,
    dest_type: str = "postgres",
) -> dict[str, Any]:
    """Build the target config for the requested registered destination type."""
    normalized = resolve_target_type(dest_type)
    builder = TARGET_CONFIG_BUILDERS[normalized]
    return builder(
        conn,
        dest_schema,
        hard_delete=hard_delete,
        source_stream=source_stream,
        dest_table=dest_table,
        emit_method=emit_method,
        upsert_key=upsert_key,
    )


def test_connection_fast(
    conn: dict[str, Any],
    *,
    source_type: str = "postgres",
) -> dict[str, Any]:
    """Run the registered fast connection test for a source connector."""
    normalized = resolve_tap_type(source_type)
    tester = CONNECTION_TESTERS[normalized]
    return tester(conn)
