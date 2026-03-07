"""CDC verification — POST /cdc/verify, POST /cdc/verify-all.

Connector-type-specific. For postgres: wal_level, wal2json, replication_role, replication_test.
"""

from __future__ import annotations

import logging

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from core.config_builder import _extract_common

logger = logging.getLogger("etl.cdc")
router = APIRouter()


def _normalize_source_type(source_type: str) -> str:
    t = (source_type or "postgres").lower()
    if t in ("source-postgres", "postgresql", "pgvector", "redshift"):
        return "postgres"
    return "postgres"


def _build_conn_params(conn: dict) -> dict:
    """Build psycopg2 connection params from config."""
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


def _verify_wal_level_postgres(conn: dict) -> dict:
    """Run SHOW wal_level and return ok if logical."""
    params = _build_conn_params(conn)
    try:
        with psycopg2.connect(**params) as pg:
            with pg.cursor() as cur:
                cur.execute("SHOW wal_level")
                row = cur.fetchone()
                wal_level = (row[0] or "").strip().lower() if row else ""
                ok = wal_level == "logical"
                return {"ok": ok, "wal_level": wal_level or "unknown"}
    except Exception as e:
        logger.warning("wal_level check failed: %s", e)
        return {"ok": False, "wal_level": "error", "error": str(e)}


def _verify_wal2json_postgres(conn: dict) -> dict:
    """Check pg_available_extensions for wal2json."""
    params = _build_conn_params(conn)
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
    except Exception as e:
        logger.warning("wal2json check failed: %s", e)
        return {"ok": False, "error": str(e)}


def _verify_replication_role_postgres(conn: dict) -> dict:
    """Attempt replication connection. Requires REPLICATION privilege."""
    params = _build_conn_params(conn)
    params["connection_factory"] = psycopg2.extras.LogicalReplicationConnection
    try:
        with psycopg2.connect(**params) as pg:
            return {"ok": True}
    except Exception as e:
        logger.warning("replication role check failed: %s", e)
        return {"ok": False, "error": str(e)}


def _verify_replication_test_postgres(conn: dict) -> dict:
    """Create temp slot, verify, drop. End-to-end replication test."""
    params = _build_conn_params(conn)
    slot_name = "mxf_cdc_test_temp"
    try:
        # pg_create_logical_replication_slot requires REPLICATION privilege
        with psycopg2.connect(**params) as pg:
            with pg.cursor() as cur:
                cur.execute(
                    "SELECT * FROM pg_create_logical_replication_slot(%s, 'wal2json')",
                    (slot_name,),
                )
            with pg.cursor() as cur:
                cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
        return {"ok": True}
    except Exception as e:
        # Try to drop slot if we created it
        try:
            with psycopg2.connect(**params) as pg:
                with pg.cursor() as cur:
                    cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name,))
        except Exception:
            pass
        logger.warning("replication test failed: %s", e)
        return {"ok": False, "error": str(e)}


class CdcVerifyRequest(BaseModel):
    source_type: str | None = None
    connection_config: dict | None = None
    step: str | None = None


@router.post("/cdc/verify")
async def cdc_verify(body: CdcVerifyRequest):
    config = body.connection_config or {}
    if not config:
        raise HTTPException(status_code=400, detail="connection_config is required")

    source_type = _normalize_source_type(body.source_type or "postgres")
    step = (body.step or "wal_level").lower()

    if source_type != "postgres":
        raise HTTPException(
            status_code=400,
            detail="Only PostgreSQL sources are supported",
        )

    handlers = {
        "wal_level": _verify_wal_level_postgres,
        "wal2json": _verify_wal2json_postgres,
        "replication_role": _verify_replication_role_postgres,
        "replication_test": _verify_replication_test_postgres,
    }
    if step not in handlers:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown step: {step}. Valid: wal_level, wal2json, replication_role, replication_test",
        )

    result = handlers[step](config)
    return result


class CdcVerifyAllRequest(BaseModel):
    source_type: str | None = None
    connection_config: dict | None = None


@router.post("/cdc/verify-all")
async def cdc_verify_all(body: CdcVerifyAllRequest):
    config = body.connection_config or {}
    if not config:
        raise HTTPException(status_code=400, detail="connection_config is required")

    source_type = _normalize_source_type(body.source_type or "postgres")

    if source_type != "postgres":
        raise HTTPException(
            status_code=400,
            detail="Only PostgreSQL sources are supported",
        )

    steps_order = ["wal_level", "wal2json", "replication_role", "replication_test"]
    handlers = {
        "wal_level": _verify_wal_level_postgres,
        "wal2json": _verify_wal2json_postgres,
        "replication_role": _verify_replication_role_postgres,
        "replication_test": _verify_replication_test_postgres,
    }
    results = {}
    all_ok = True
    for step in steps_order:
        r = handlers[step](config)
        results[step] = r
        if not r.get("ok", False):
            all_ok = False
            if step in ("wal_level", "replication_role"):
                break  # Skip remaining if critical step fails

    return {
        "ok": all_ok,
        "steps": results,
        "overall": "verified" if all_ok else "failed",
    }
