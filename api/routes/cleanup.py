"""Connection cleanup — POST /cleanup/connection.

Drops replication slot when a data source connection is deleted.
Connector-type-specific. For postgres: pg_drop_replication_slot.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

import psycopg2

from core.connector_support import normalize_source_type
from core.postgres_admin import drop_replication_slot
from core.source_mutation_policy import are_source_db_mutations_allowed

logger = logging.getLogger("etl.cleanup")
router = APIRouter()

def _require_supported_cleanup_source(source_type: str | None) -> str:
    normalized = normalize_source_type(source_type)
    if normalized != "postgres":
        raise HTTPException(
            status_code=400,
            detail=f"Connection cleanup is not implemented for source_type={source_type!r}",
        )
    return normalized


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
    if not are_source_db_mutations_allowed():
        return {
            "ok": True,
            "message": "Skipped replication slot cleanup because source DB mutations are disabled by policy",
        }

    _require_supported_cleanup_source(body.source_type)
    try:
        return drop_replication_slot(config, slot_name)
    except psycopg2.Error as e:
        logger.warning("Failed to drop slot %s: %s", slot_name, e)
        raise HTTPException(status_code=500, detail=str(e))
