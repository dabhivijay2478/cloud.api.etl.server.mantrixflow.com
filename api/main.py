"""
MANTrixFlow ETL — dlt Data Load Tool
Sources: PostgreSQL, MongoDB | Destination: PostgreSQL
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import health, connectors, discover, preview, sync, test_connection

app = FastAPI(
    title="MANTrixFlow ETL",
    description="dlt-based Collect → Transform → Emit",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(health.router, tags=["health"])
app.include_router(connectors.router, prefix="/connectors", tags=["connectors"])
app.include_router(discover.router, tags=["discover"])
app.include_router(preview.router, tags=["preview"])
app.include_router(sync.router, prefix="/sync", tags=["sync"])
app.include_router(test_connection.router, tags=["test-connection"])
