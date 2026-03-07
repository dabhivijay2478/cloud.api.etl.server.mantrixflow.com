"""Connection cleanup — POST /cleanup/connection.

Drops replication slot when a data source connection is deleted.
Connector-type-specific. For postgres: pg_drop_replication_slot.
"""

from __future__ import annotations

import logging

import psycopg2
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from core.config_builder import _extract_common

logger = logging.getLogger("etl.cleanup")
router = APIRouter()


def _normalize_source_type(source_type: str) -> str:
    t = (source_type or "postgres").lower()
    if t in ("source-postgres", "postgresql", "pgvector", "redshift"):
        return "postgres"
    return "postgres"


def _build_conn_params(conn: dict) -> dict:
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


class CleanupConnectionRequest(BaseModel):
    source_type: str | None = None
    connection_config: dict | None = None
    replication_slot_name: str | None = None


@router.post("/cleanup/connection")
async def cleanup_connection(body: CleanupConnectionRequest):
    config = body.connection_config or {}
    slot_name = body.replication_slot_name

    if not slot_name or not slot_name.strip():
        return {"ok": True, "message": "No replication slot to drop"}

    source_type = _normalize_source_type(body.source_type or "postgres")

    if source_type != "postgres":
        raise HTTPException(
            status_code=400,
            detail="Only PostgreSQL sources are supported",
        )

    params = _build_conn_params(config)
    try:
        with psycopg2.connect(**params) as pg:
            with pg.cursor() as cur:
                cur.execute("SELECT pg_drop_replication_slot(%s)", (slot_name.strip(),))
        logger.info("Dropped replication slot: %s", slot_name)
        return {"ok": True, "message": f"Dropped slot {slot_name}"}
    except psycopg2.Error as e:
        # Slot might already be dropped
        if "does not exist" in str(e) or "replication slot" in str(e).lower():
            logger.info("Slot %s already dropped or not found: %s", slot_name, e)
            return {"ok": True, "message": f"Slot {slot_name} already dropped"}
        logger.warning("Failed to drop slot %s: %s", slot_name, e)
        raise HTTPException(status_code=500, detail=str(e))
