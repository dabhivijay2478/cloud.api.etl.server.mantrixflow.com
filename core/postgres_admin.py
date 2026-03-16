"""PostgreSQL-only admin helpers used by CDC verification and cleanup routes."""

from __future__ import annotations

import logging

import psycopg2
import psycopg2.extras

from core.connection_utils import build_sqlalchemy_url

logger = logging.getLogger("etl.postgres_admin")


def build_postgres_dsn(conn: dict) -> str:
    return build_sqlalchemy_url("postgres", conn).replace("postgresql+psycopg2://", "postgresql://", 1)


def verify_wal_level(conn: dict) -> dict:
    params = build_postgres_dsn(conn)
    try:
        with psycopg2.connect(params) as pg:
            with pg.cursor() as cur:
                cur.execute("SHOW wal_level")
                row = cur.fetchone()
                wal_level = (row[0] or "").strip().lower() if row else ""
                return {
                    "ok": wal_level == "logical",
                    "wal_level": wal_level or "unknown",
                }
    except Exception as exc:
        logger.warning("wal_level check failed: %s", exc)
        return {"ok": False, "wal_level": "error", "error": str(exc)}


def verify_pgoutput(conn: dict) -> dict:
    params = build_postgres_dsn(conn)
    try:
        with psycopg2.connect(params) as pg:
            with pg.cursor() as cur:
                cur.execute("SHOW server_version_num")
                row = cur.fetchone()
                version = int(row[0]) if row and row[0] else 0
                return {
                    "ok": version >= 100000,
                    "plugin": "pgoutput",
                    "server_version_num": version,
                }
    except Exception as exc:
        logger.warning("pgoutput check failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def verify_replication_role(conn: dict) -> dict:
    params = build_postgres_dsn(conn)
    try:
        with psycopg2.connect(
            params,
            connection_factory=psycopg2.extras.LogicalReplicationConnection,
        ):
            return {"ok": True}
    except Exception as exc:
        logger.warning("replication role check failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def verify_replication_test(conn: dict) -> dict:
    params = build_postgres_dsn(conn)
    slot_name = "mxf_cdc_test_temp"
    try:
        with psycopg2.connect(params) as pg:
            with pg.cursor() as cur:
                cur.execute(
                    "SELECT * FROM pg_create_logical_replication_slot(%s, 'pgoutput')",
                    (slot_name,),
                )
            with pg.cursor() as cur:
                cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
        return {"ok": True}
    except Exception as exc:
        # Best-effort cleanup of the slot we may have just created
        try:
            with psycopg2.connect(params) as pg:
                with pg.cursor() as cur:
                    cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
        except Exception:
            pass
        logger.warning("replication test failed: %s", exc)
        return {"ok": False, "error": str(exc)}


def drop_replication_slot(conn: dict, slot_name: str) -> dict:
    params = build_postgres_dsn(conn)
    try:
        with psycopg2.connect(params) as pg:
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
