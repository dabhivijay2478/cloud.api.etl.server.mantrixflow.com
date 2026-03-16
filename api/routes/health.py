"""Health check — POST /health.

Returns status, version, and dlt_version. No auth required.
"""

from __future__ import annotations

import dlt
from fastapi import APIRouter

from core.concurrency import get_capacity

router = APIRouter()


@router.post("/health")
@router.get("/health")
async def health():
    capacity = get_capacity()
    return {
        "status": "ok",
        "version": "1.0.0",
        "dlt_version": getattr(dlt, "__version__", "unknown"),
        **capacity,
    }
