"""Build tap-postgres and target-postgres config JSON dicts from connection credentials.

Single source of truth for ALL PostgreSQL connection resolution in this ETL server.
Every route and module that needs a DB connection must go through this module.

Connection config fields accepted (all NestJS key variants supported):
  host / hostname         → host
  port                    → port             (default: 5432)
  user / username         → user
  password                → password
  database / dbname       → database

SSL — driven entirely by the ``ssl`` field; no host-pattern guessing:
  ssl: true / "true"      → sslmode=require
  ssl: false / "false"    → sslmode=disable
  ssl: {enabled: true}    → sslmode=require
  ssl: {require: true}    → sslmode=require
  ssl: absent / null      → sslmode=prefer   (server decides)

Transaction pooler / pgBouncer — driven by explicit config fields:
  pgbouncer: true                      → disable prepared statements
  connection_type: "transaction_pooler"→ disable prepared statements
  (psycopg3 URL gets prepare_threshold=0 so no PREPARE is ever sent)

Optional:
  connect_timeout          → seconds before giving up (default: 10)
"""

from __future__ import annotations

import logging
from typing import Any
from urllib.parse import quote_plus

import psycopg2

from core.connector_support import normalize_dest_type, normalize_source_type

logger = logging.getLogger("etl.config")

# Default timeouts (seconds) — used when the conn dict has no connect_timeout
_DEFAULT_CONNECT_TIMEOUT = 10
_DEFAULT_PSYCOPG_TIMEOUT = 15


# ──────────────────────────────────────────────────────────────────────────────
# SSL field parser
# ──────────────────────────────────────────────────────────────────────────────

def _parse_ssl(ssl: Any) -> bool | None:
    """Parse the raw ``ssl`` connection field.

    Returns:
        True   — SSL explicitly required.
        False  — SSL explicitly disabled.
        None   — No setting provided; use sslmode=prefer.
    """
    if ssl is None:
        return None
    if isinstance(ssl, bool):
        return ssl
    if isinstance(ssl, str):
        v = ssl.strip().lower()
        if v in ("true", "1", "yes", "on", "require"):
            return True
        if v in ("false", "0", "no", "off", "disable"):
            return False
        return None
    if isinstance(ssl, dict):
        # Explicit disable wins first
        if ssl.get("enabled") is False or ssl.get("require") is False:
            return False
        # Any truthy key means require
        if any(ssl.get(k) for k in ("enabled", "require", "ca_cert", "client_cert", "client_key")):
            return True
        return None  # empty dict → no opinion
    return None


# ──────────────────────────────────────────────────────────────────────────────
# Pooler detection — explicit config fields only
# ──────────────────────────────────────────────────────────────────────────────

def _is_transaction_pooler(conn: dict[str, Any]) -> bool:
    """Return True when the caller has explicitly flagged a transaction pooler.

    Checks:
      conn["pgbouncer"] is truthy
      conn["connection_type"] contains "transaction"  (case-insensitive)
    """
    if conn.get("pgbouncer"):
        return True
    conn_type = str(conn.get("connection_type") or "").lower()
    return "transaction" in conn_type


# ──────────────────────────────────────────────────────────────────────────────
# URL / DSN builders
# ──────────────────────────────────────────────────────────────────────────────

def _build_sqlalchemy_url(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str,
    *,
    needs_ssl: bool,
    transaction_pooler: bool,
) -> str:
    """Build a SQLAlchemy postgresql:// URL for psycopg3 (Singer SDK driver).

    Every credential component is percent-encoded.
    Appends:
      sslmode=require        when needs_ssl is True
      sslmode=prefer         when needs_ssl is False (server decides)
      prepare_threshold=0    when transaction_pooler is True
    """
    user_enc = quote_plus(user)
    pwd_enc = quote_plus(password)
    db_enc = quote_plus(database)

    sslmode = "require" if needs_ssl else "prefer"
    params: list[str] = [f"sslmode={sslmode}"]
    if transaction_pooler:
        params.append("prepare_threshold=0")

    return f"postgresql://{user_enc}:{pwd_enc}@{host}:{port}/{db_enc}?{'&'.join(params)}"


def build_psycopg_dsn(conn: dict[str, Any]) -> str:
    """Build a libpq DSN for direct psycopg3 connections (introspect, admin, etc).

    Uses the same field resolution as _extract_common so every caller gets
    identical SSL/pooler/encoding behaviour.
    """
    cfg = _extract_common(conn)
    user_enc = quote_plus(cfg["user"])
    pwd_enc = quote_plus(cfg["password"])
    db_enc = quote_plus(cfg["database"])
    sslmode = "require" if cfg["ssl_enable"] else "prefer"
    timeout = cfg["connect_timeout"]

    params: list[str] = [
        f"sslmode={sslmode}",
        f"connect_timeout={timeout}",
    ]
    if cfg["transaction_pooler"]:
        params.append("prepare_threshold=0")

    return (
        f"postgresql://{user_enc}:{pwd_enc}@{cfg['host']}:{cfg['port']}/{db_enc}"
        f"?{'&'.join(params)}"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Core connection resolver  ← everything flows through here
# ──────────────────────────────────────────────────────────────────────────────

def _extract_common(conn: dict[str, Any]) -> dict[str, Any]:
    """Resolve all PostgreSQL connection fields from the NestJS connection config.

    Accepts every key variant the API may send.
    Returns a canonical dict consumed by tap/target config builders and
    direct psycopg callers.

    Returned keys:
      host, port, user, password, database
      ssl_enable (bool)
      ssl_mode   ("require" | "prefer")
      transaction_pooler (bool)
      connect_timeout (int, seconds)
      sqlalchemy_url  (str, for Singer subprocess chain)
    """
    # ── credentials ──────────────────────────────────────────────────────────
    host = str(conn.get("host") or conn.get("hostname") or "localhost").strip()
    port = int(conn.get("port") or 5432)
    user = str(conn.get("user") or conn.get("username") or "postgres").strip()
    password = str(conn.get("password") or "")
    database = str(conn.get("database") or conn.get("dbname") or "postgres").strip()

    # ── SSL ───────────────────────────────────────────────────────────────────
    explicit = _parse_ssl(conn.get("ssl"))
    needs_ssl: bool = explicit if explicit is not None else False
    # sslmode=prefer when no opinion → psycopg / libpq tries SSL but falls back

    # ── pooler ────────────────────────────────────────────────────────────────
    txpool = _is_transaction_pooler(conn)

    # ── connect timeout ───────────────────────────────────────────────────────
    connect_timeout = int(conn.get("connect_timeout") or _DEFAULT_CONNECT_TIMEOUT)

    cfg: dict[str, Any] = {
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
        "ssl_enable": needs_ssl,
        "ssl_mode": "require" if needs_ssl else "prefer",
        "transaction_pooler": txpool,
        "connect_timeout": connect_timeout,
        "sqlalchemy_url": _build_sqlalchemy_url(
            host, port, user, password, database,
            needs_ssl=needs_ssl,
            transaction_pooler=txpool,
        ),
    }

    logger.info(
        "Built config: host=%s port=%d user=%s database=%s ssl=%s pooler=%s "
        "(input keys: %s)",
        host, port, user, database, needs_ssl, txpool, sorted(conn.keys()),
    )
    return cfg


# ──────────────────────────────────────────────────────────────────────────────
# Tap / target config builders
# ──────────────────────────────────────────────────────────────────────────────

def _build_postgres_tap_config(
    conn: dict[str, Any],
    replication_slot_name: str | None = None,
) -> dict[str, Any]:
    """Build config dict for the tap-postgres Singer subprocess."""
    config = _extract_common(conn)
    if replication_slot_name:
        config["replication_slot_name"] = replication_slot_name
    return config


def _build_postgres_target_config(
    conn: dict[str, Any],
    dest_schema: str,
    *,
    hard_delete: bool = False,
    source_stream: str | None = None,
    dest_table: str | None = None,
    emit_method: str = "append",
    upsert_key: list[str] | None = None,
) -> dict[str, Any]:
    """Build config dict for the target-postgres Singer subprocess.

    dest_schema and dest_table are passed in at call time — never defaulted here.
    stream_maps is added when the source stream name differs from the dest table name.
    """
    config = _extract_common(conn)
    config["default_target_schema"] = dest_schema
    config["add_record_metadata"] = False
    config["activate_version"] = False
    config["hard_delete"] = hard_delete
    config["load_method"] = _map_emit_method_to_load_method(emit_method)

    if source_stream and dest_table and source_stream != dest_table:
        config["stream_maps"] = {source_stream: {"__alias__": dest_table}}
        logger.info("stream_maps: '%s' -> '%s'", source_stream, dest_table)

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
    """Test connectivity with a quick SELECT 1 via psycopg2.

    Avoids spawning the tap subprocess (which runs full discovery) and
    completes in well under the NestJS HTTP timeout.
    """
    cfg = _extract_common(conn)
    sslmode = cfg["ssl_mode"]  # "require" or "prefer"
    try:
        with psycopg2.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"],
            dbname=cfg["database"],
            connect_timeout=cfg["connect_timeout"],
            sslmode=sslmode,
        ) as pg_conn:
            with pg_conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        return {"success": True}
    except Exception as exc:
        err = str(exc).strip() or type(exc).__name__
        logger.warning(
            "Connection test failed for %s:%d/%s: %s",
            cfg["host"], cfg["port"], cfg["database"], err,
        )
        return {"success": False, "error": err}


# ──────────────────────────────────────────────────────────────────────────────
# Connector registries
# ──────────────────────────────────────────────────────────────────────────────

TAP_CONFIG_BUILDERS = {
    "postgres": _build_postgres_tap_config,
}

TARGET_CONFIG_BUILDERS = {
    "postgres": _build_postgres_target_config,
}

CONNECTION_TESTERS = {
    "postgres": _test_postgres_connection_fast,
}


# ──────────────────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────────────────

def resolve_tap_type(source_type: str | None = None) -> str:
    normalized = normalize_source_type(source_type)
    if normalized not in TAP_CONFIG_BUILDERS:
        supported = ", ".join(sorted(TAP_CONFIG_BUILDERS))
        raise ValueError(
            f"Unsupported source_type={source_type!r}. Supported: {supported}"
        )
    return normalized


def resolve_target_type(dest_type: str | None = None) -> str:
    normalized = normalize_dest_type(dest_type)
    if normalized not in TARGET_CONFIG_BUILDERS:
        supported = ", ".join(sorted(TARGET_CONFIG_BUILDERS))
        raise ValueError(
            f"Unsupported dest_type={dest_type!r}. Supported: {supported}"
        )
    return normalized


def build_tap_config(
    conn: dict[str, Any],
    replication_slot_name: str | None = None,
    *,
    source_type: str = "postgres",
) -> dict[str, Any]:
    return TAP_CONFIG_BUILDERS[resolve_tap_type(source_type)](conn, replication_slot_name)


def build_target_config(
    conn: dict[str, Any],
    dest_schema: str,
    *,
    hard_delete: bool = False,
    source_stream: str | None = None,
    dest_table: str | None = None,
    emit_method: str = "append",
    upsert_key: list[str] | None = None,
    dest_type: str = "postgres",
) -> dict[str, Any]:
    return TARGET_CONFIG_BUILDERS[resolve_target_type(dest_type)](
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
    return CONNECTION_TESTERS[resolve_tap_type(source_type)](conn)
