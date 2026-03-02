"""Health check endpoint."""

from fastapi import APIRouter

router = APIRouter()


@router.get("/health")
def health():
    """Health check for ETL service."""
    return {"status": "ok", "service": "mantrixflow-etl"}
