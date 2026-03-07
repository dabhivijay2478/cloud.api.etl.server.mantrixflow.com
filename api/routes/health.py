"""Health check — POST /health.

Returns pod capacity info for K8s readiness probes and NestJS health checks.
"""

from fastapi import APIRouter

from core.concurrency import get_capacity

router = APIRouter()


@router.post("/health")
@router.get("/health")
async def health():
    capacity = get_capacity()
    return {"status": "ok", **capacity}
