# MANTrixFlow ETL Service - Multi-stage build with Meltano
# Phase 6: Immutable runtime with Meltano + plugins baked in

# -----------------------------------------------------------------------------
# Stage 1: Build - Install Meltano and plugins
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS meltano-builder

WORKDIR /app

# Install system deps for Meltano and taps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Meltano
RUN pip install --no-cache-dir meltano

# Copy Meltano project files first (for layer caching)
COPY meltano.yml ./
COPY transform/ ./transform/

# Install Meltano plugins (tap-postgres, tap-mysql, tap-mongodb, target-postgres, dbt-postgres)
RUN meltano install

# -----------------------------------------------------------------------------
# Stage 2: Runtime - FastAPI + Meltano
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS runtime

WORKDIR /app

# Install runtime deps (curl for healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy Python requirements
COPY requirements.txt ./

# Install Python deps
RUN pip install --no-cache-dir -r requirements.txt

# Install Singer taps (local connectors - used when Meltano path unavailable)
COPY connectors/ ./connectors/
RUN pip install --no-cache-dir -e ./connectors/tap-postgres -e ./connectors/tap-mysql -e ./connectors/tap-mongodb 2>/dev/null || true

# Copy Meltano project and installed plugins from builder
COPY --from=meltano-builder /app/.meltano /app/.meltano
COPY --from=meltano-builder /app/meltano.yml ./
COPY --from=meltano-builder /app/transform ./transform

# Copy application code
COPY main.py ./
COPY run.py ./
COPY transformer.py ./
COPY etl_logger.py ./
COPY utils.py ./
COPY orchestration/ ./orchestration/
COPY api/ ./api/

# Ensure Meltano project is valid
RUN meltano config 2>/dev/null || true

# Expose port
ENV PORT=8001
EXPOSE ${PORT}

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# Start FastAPI
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
