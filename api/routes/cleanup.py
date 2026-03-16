"""Connection cleanup — POST /cleanup/connection.

Drops replication slot when a data source connection is deleted.
Connector-type-specific. For postgres: pg_drop_replication_slot.
"""

from __future__ import annotations

import logging

import psycopg2
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core.config import settings
from core.connector_support import normalize_source_type
from core.encryption import decrypt_credentials
from core.postgres_admin import drop_replication_slot
from core.security import validate_etl_token
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
    slot_name: str | None = None
    pg_conn_str_encrypted: str | None = None


@router.post("/cleanup/connection", dependencies=[Depends(validate_etl_token)])
async def cleanup_connection(body: CleanupConnectionRequest):
    slot_name = body.slot_name or body.replication_slot_name
    if not slot_name or not slot_name.strip():
        return {"ok": True, "message": "No replication slot to drop"}
    if not are_source_db_mutations_allowed():
        return {
            "ok": True,
            "message": "Skipped replication slot cleanup because source DB mutations are disabled by policy",
        }

    config = body.connection_config or {}
    if body.pg_conn_str_encrypted and settings.ENCRYPTION_KEY:
        try:
            decrypted = decrypt_credentials(body.pg_conn_str_encrypted, settings.ENCRYPTION_KEY)
            if isinstance(decrypted, dict):
                config = decrypted
            else:
                config = {"connection_url": str(decrypted)}
        except Exception as exc:
            logger.warning("Failed to decrypt connection string: %s", exc)
            raise HTTPException(status_code=400, detail="Invalid encrypted connection string")

    _require_supported_cleanup_source(body.source_type)
    try:
        return drop_replication_slot(config, slot_name)
    except psycopg2.Error as e:
        logger.warning("Failed to drop slot %s: %s", slot_name, e)
        raise HTTPException(status_code=500, detail=str(e))
