"""Connector metadata and CDC setup."""

from fastapi import APIRouter, HTTPException

router = APIRouter()

SOURCES = [
    {"id": "source-postgres", "label": "PostgreSQL", "category": "Database", "cdc": True},
    {"id": "source-mongodb-v2", "label": "MongoDB", "category": "Database", "cdc": False},
]
DESTINATIONS = [
    {"id": "postgres", "label": "PostgreSQL"},
]


@router.get("")
def list_connectors():
    """List available sources and destinations."""
    return {"sources": SOURCES, "destinations": DESTINATIONS}


@router.get("/{source_type}/cdc-setup")
def get_cdc_setup(source_type: str):
    """CDC setup info (Postgres only — pg_replication)."""
    if source_type in ("source-postgres", "postgres", "postgresql"):
        return {
            "source_type": "postgres",
            "mechanism": "WAL logical replication",
            "requirements": [
                "wal_level=logical",
                "Replication slot",
                "Publication",
                "Replica identity on tables",
            ],
            "docs": "https://dlthub.com/docs/dlt-ecosystem/verified-sources/pg_replication",
        }
    if source_type in ("source-mongodb-v2", "mongodb"):
        return {
            "source_type": "mongodb",
            "mechanism": "Cursor-based incremental (dlt.sources.incremental)",
            "requirements": ["cursor_field (e.g. _id, updatedAt)"],
            "docs": "https://dlthub.com/docs/dlt-ecosystem/verified-sources/mongodb",
        }
    raise HTTPException(status_code=404, detail=f"CDC setup not found for {source_type}")
