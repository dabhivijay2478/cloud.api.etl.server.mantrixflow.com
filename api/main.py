"""MANTrixFlow ETL Server — dlt-based extraction, load, and transform service."""

from __future__ import annotations

from dotenv import load_dotenv

load_dotenv()

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

from api.routes import health, test_connection, discover, preview, sync, introspect_table, cdc_verify, cleanup
from core.dlt_runner import cleanup_stale_tmpfs, drain_running_pipelines

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("etl")


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("ETL server starting up")
    cleanup_stale_tmpfs()
    yield
    logger.info("ETL server shutting down — draining pipelines")
    await drain_running_pipelines(timeout=300)
    logger.info("ETL server shutdown complete")


app = FastAPI(
    title="MANTrixFlow ETL Server",
    description="dlt-based ETL service for SQL database pipelines",
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = time.monotonic()
    logger.info("%s %s", request.method, request.url.path)
    response = await call_next(request)
    elapsed = round((time.monotonic() - start) * 1000)
    logger.info(
        "%s %s → %d (%dms)",
        request.method,
        request.url.path,
        response.status_code,
        elapsed,
    )
    return response


app.include_router(health.router, tags=["health"])
app.include_router(test_connection.router, tags=["test-connection"])
app.include_router(discover.router, tags=["discover"])
app.include_router(preview.router, tags=["preview"])
app.include_router(sync.router, tags=["sync"])
app.include_router(introspect_table.router, tags=["introspect"])
app.include_router(cdc_verify.router, tags=["cdc"])
app.include_router(cleanup.router, tags=["cleanup"])
