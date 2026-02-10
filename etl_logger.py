"""
Structured JSON logging for the Python ETL service.
Mirrors the NestJS ActivityLoggerService format for consistent log aggregation.

Usage:
    from etl_logger import logger, activity

    logger.info("Server starting", extra={"port": 8001})
    activity("sync.collect", "Collected 500 rows", source_type="postgresql", metadata={"rows": 500})
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from typing import Any, Optional


class JSONFormatter(logging.Formatter):
    """Outputs each log record as a single JSON line (compatible with Pino output)."""

    def format(self, record: logging.LogRecord) -> str:
        log_entry: dict[str, Any] = {
            "level": record.levelname.lower(),
            "time": datetime.now(timezone.utc).isoformat(),
            "service": "python-etl",
            "msg": record.getMessage(),
        }

        # Merge any extra fields passed via `extra={}` or `activity()`
        for key in ("action", "userId", "pipelineId", "runId", "organizationId", "sourceType", "metadata"):
            val = getattr(record, key, None)
            if val is not None:
                log_entry[key] = val

        # Include exception info if present
        if record.exc_info and record.exc_info[0] is not None:
            log_entry["error"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


def _create_logger() -> logging.Logger:
    """Create the root ETL logger with JSON output."""
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()
    _logger = logging.getLogger("python-etl")
    _logger.setLevel(getattr(logging, log_level, logging.INFO))

    # Avoid duplicate handlers on module reload
    if not _logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(JSONFormatter())
        _logger.addHandler(handler)

    # Prevent propagation to root logger (avoids duplicate uvicorn output)
    _logger.propagate = False
    return _logger


logger = _create_logger()


def activity(
    action: str,
    message: str,
    *,
    level: str = "info",
    user_id: Optional[str] = None,
    pipeline_id: Optional[str] = None,
    run_id: Optional[str] = None,
    organization_id: Optional[str] = None,
    source_type: Optional[str] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> None:
    """
    Log a structured activity event (mirrors NestJS ActivityLoggerService).

    Examples:
        activity("sync.collect", "Collected 500 rows", source_type="postgresql")
        activity("request.error", "Connection failed", level="error", metadata={"host": "db"})
    """
    extra: dict[str, Any] = {"action": action}
    if user_id:
        extra["userId"] = user_id
    if pipeline_id:
        extra["pipelineId"] = pipeline_id
    if run_id:
        extra["runId"] = run_id
    if organization_id:
        extra["organizationId"] = organization_id
    if source_type:
        extra["sourceType"] = source_type
    if metadata:
        extra["metadata"] = metadata

    log_fn = getattr(logger, level, logger.info)
    log_fn(message, extra=extra)
