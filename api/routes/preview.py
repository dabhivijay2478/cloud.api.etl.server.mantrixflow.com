"""Preview — POST /preview.

Runs tap in FULL_TABLE mode through transformer, collects N records.
No target, no destination write.
"""

import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from core.singer_runner import run_preview

logger = logging.getLogger("etl.preview")
router = APIRouter()


class PreviewRequest(BaseModel):
    connection_config: dict | None = None
    source_config: dict | None = None
    source_stream: str
    limit: int = 50
    column_map: dict[str, str] | None = None
    drop_columns: list[str] | None = None
    transform_script: str | None = None
    source_type: str | None = None


@router.post("/preview")
async def preview(body: PreviewRequest):
    config = body.connection_config or body.source_config or {}
    if not config:
        raise HTTPException(status_code=400, detail="connection_config is required")
    if not body.source_stream:
        raise HTTPException(status_code=400, detail="source_stream is required")

    logger.info(
        "Preview requested: stream=%s limit=%d",
        body.source_stream, body.limit,
    )

    try:
        result = await run_preview(
            source_connection_config=config,
            source_stream=body.source_stream,
            limit=body.limit,
            column_map=body.column_map,
            drop_columns=body.drop_columns,
            transform_script=body.transform_script,
        )
    except RuntimeError as exc:
        logger.error("Preview failed for stream %s: %s", body.source_stream, exc)
        raise HTTPException(status_code=500, detail=str(exc))

    logger.info(
        "Preview complete: stream=%s rows=%d columns=%d",
        body.source_stream,
        result.get("total", 0),
        len(result.get("columns", [])),
    )

    return result
