"""CDC verification — POST /cdc/verify, POST /cdc/verify-all.

Connector-type-specific. For postgres: wal_level, wal2json, replication_role, replication_test.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from core.connector_support import normalize_source_type
from core.postgres_admin import (
    verify_replication_role,
    verify_replication_test,
    verify_wal2json,
    verify_wal_level,
)
from core.source_mutation_policy import (
    SOURCE_DB_MUTATION_POLICY_MESSAGE,
    are_source_db_mutations_allowed,
)

router = APIRouter()

def _require_supported_cdc_source(source_type: str | None) -> str:
    normalized = normalize_source_type(source_type)
    if normalized != "postgres":
        raise HTTPException(
            status_code=400,
            detail=f"CDC verification is not implemented for source_type={source_type!r}",
        )
    return normalized


class CdcVerifyRequest(BaseModel):
    source_type: str | None = None
    connection_config: dict | None = None
    step: str | None = None


@router.post("/cdc/verify")
async def cdc_verify(body: CdcVerifyRequest):
    config = body.connection_config or {}
    if not config:
        raise HTTPException(status_code=400, detail="connection_config is required")

    _require_supported_cdc_source(body.source_type)
    step = (body.step or "wal_level").lower()
    if step == "replication_test" and not are_source_db_mutations_allowed():
        raise HTTPException(status_code=400, detail=SOURCE_DB_MUTATION_POLICY_MESSAGE)

    handlers = {
        "wal_level": verify_wal_level,
        "wal2json": verify_wal2json,
        "replication_role": verify_replication_role,
        "replication_test": verify_replication_test,
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

    _require_supported_cdc_source(body.source_type)
    if not are_source_db_mutations_allowed():
        return {
            "ok": False,
            "steps": {
                "wal_level": verify_wal_level(config),
                "wal2json": verify_wal2json(config),
                "replication_role": verify_replication_role(config),
                "replication_test": {
                    "ok": False,
                    "error": SOURCE_DB_MUTATION_POLICY_MESSAGE,
                },
            },
            "overall": "failed",
        }

    steps_order = ["wal_level", "wal2json", "replication_role", "replication_test"]
    handlers = {
        "wal_level": verify_wal_level,
        "wal2json": verify_wal2json,
        "replication_role": verify_replication_role,
        "replication_test": verify_replication_test,
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
