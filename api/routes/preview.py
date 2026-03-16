"""Preview — POST /preview."""

from __future__ import annotations

import logging

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from core.connector_support import normalize_source_type
from core.dlt_runner import run_preview
from core.security import validate_etl_token

logger = logging.getLogger("etl.preview")
router = APIRouter()


class PreviewRequest(BaseModel):
    connection_config: dict | None = None
    source_stream: str
    selected_streams: list[str] | None = None
    limit: int = 50
    column_map: dict[str, str] | None = None
    drop_columns: list[str] | None = None
    transform_script: str | None = None
    source_type: str | None = None
    connector_type: str | None = None
    on_transform_error: str = "fail"


@router.post("/preview", dependencies=[Depends(validate_etl_token)])
async def preview(body: PreviewRequest):
    config = body.connection_config or {}
    if not config:
        raise HTTPException(status_code=400, detail="connection_config is required")
    stream = body.source_stream or (body.selected_streams[0] if body.selected_streams else None)
    if not stream:
        raise HTTPException(status_code=400, detail="source_stream or selected_streams is required")

    logger.info(
        "Preview requested: stream=%s limit=%d",
        stream, body.limit,
    )

    try:
        st = body.connector_type or body.source_type
        source_type = normalize_source_type(st)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    try:
        result = await run_preview(
            source_connection_config=config,
            source_stream=stream,
            limit=body.limit,
            column_map=body.column_map,
            drop_columns=body.drop_columns,
            transform_script=body.transform_script,
            source_type=source_type,
            on_transform_error=body.on_transform_error,
        )
    except RuntimeError as exc:
        logger.error("Preview failed for stream %s: %s", stream, exc)
        raise HTTPException(status_code=500, detail=str(exc))

    logger.info(
        "Preview complete: stream=%s rows=%d columns=%d",
        stream,
        result.get("total", 0),
        len(result.get("columns", [])),
    )

    return result
