"""PostgreSQL-only admin helpers used by CDC verification and cleanup routes."""

from __future__ import annotations

import logging

import psycopg2
import psycopg2.extras

from core.config_builder import _extract_common

logger = logging.getLogger("etl.postgres_admin")


def build_postgres_conn_params(conn: dict) -> dict:
    """Build psycopg2 connection params from stored connector config."""
    cfg = _extract_common(conn)
    params = {
        "host": cfg.get("host", "localhost"),
        "port": int(cfg.get("port", 5432)),
        "user": cfg.get("user", "postgres"),
        "password": cfg.get("password", ""),
        "dbname": cfg.get("database", cfg.get("dbname", "postgres")),
    }
    if cfg.get("ssl_enable"):
        params["sslmode"] = "require"
    return params


def verify_wal_level(conn: dict) -> dict:
    params = build_postgres_conn_params(conn)
    try:
        with psycopg2.connect(**params) as pg:
            with pg.cursor() as cur:
                cur.execute("SHOW wal_level")
                row = cur.fetchone()
                wal_level = (row[0] or "").strip().lower() if row else ""
                return {"ok": wal_level == "logical", "wal_level": wal_level or "unknown"}
    except Exception as exc:
        logger.warning("wal_level check failed: %s", exc)
        return {"ok": False, "wal_level": "error", "error": str(exc)}


def verify_wal2json(conn: dict) -> dict:
    params = build_postgres_conn_params(conn)
    try:
        with psycopg2.connect(**params) as pg:
            with pg.cursor() as cur:
                cur.execute(
                    "SELECT name, installed_version FROM pg_available_extensions WHERE name = 'wal2json'"
                )
                row = cur.fetchone()
                if row and row[1]:
                    return {"ok": True, "installed_version": row[1]}
                return {"ok": False, "installed_version": None}
    except Exception as exc:
        logger.warning("wal2json check failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def verify_replication_role(conn: dict) -> dict:
    params = build_postgres_conn_params(conn)
    params["connection_factory"] = psycopg2.extras.LogicalReplicationConnection
    try:
        with psycopg2.connect(**params):
            return {"ok": True}
    except Exception as exc:
        logger.warning("replication role check failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def verify_replication_test(conn: dict) -> dict:
    params = build_postgres_conn_params(conn)
    slot_name = "mxf_cdc_test_temp"
    try:
        with psycopg2.connect(**params) as pg:
            with pg.cursor() as cur:
                cur.execute(
                    "SELECT * FROM pg_create_logical_replication_slot(%s, 'wal2json')",
                    (slot_name,),
                )
            with pg.cursor() as cur:
                cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
        return {"ok": True}
    except Exception as exc:
        try:
            with psycopg2.connect(**params) as pg:
                with pg.cursor() as cur:
                    cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
        except Exception:
            pass
        logger.warning("replication test failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def drop_replication_slot(conn: dict, slot_name: str) -> dict:
    params = build_postgres_conn_params(conn)
    try:
        with psycopg2.connect(**params) as pg:
            with pg.cursor() as cur:
                cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name.strip(),))
        logger.info("Dropped replication slot: %s", slot_name)
        return {"ok": True, "message": f"Dropped slot {slot_name}"}
    except psycopg2.Error as exc:
        if "does not exist" in str(exc) or "replication slot" in str(exc).lower():
            logger.info("Slot %s already dropped or not found: %s", slot_name, exc)
            return {"ok": True, "message": f"Slot {slot_name} already dropped"}
        logger.warning("Failed to drop slot %s: %s", slot_name, exc)
        raise
