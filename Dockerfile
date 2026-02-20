# MANTrixFlow ETL Service - Multi-stage build with Meltano
# Phase 6: Immutable runtime with Meltano + plugins baked in

# -----------------------------------------------------------------------------
# Stage 1: Build - Install Meltano and plugins
# -----------------------------------------------------------------------------
FROM python:3.11-slim AS meltano-builder

WORKDIR /app

# Install system deps for Meltano, taps, and target-mysql (mysqlclient)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    curl \
    pkg-config \
    default-libmysqlclient-dev \
    && rm -rf /var/lib/apt/lists/*

# Install Meltano (Python 3.11 avoids pendulum/distutils issues on 3.12)
RUN pip install --no-cache-dir meltano

# Copy Meltano project files first (for layer caching)
COPY meltano.yml ./
COPY plugins/ ./plugins/
COPY transform/ ./transform/

# Install Meltano plugins (all taps, targets, dbt-postgres)
RUN meltano install

# -----------------------------------------------------------------------------
# Stage 2: Runtime - FastAPI + Meltano
# -----------------------------------------------------------------------------
FROM python:3.11-slim AS runtime

WORKDIR /app

# Install runtime deps (curl for healthcheck)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy Python requirements
COPY requirements.txt ./

# Install Python deps (FastAPI, uvicorn, etc. - taps/targets managed by Meltano)
RUN pip install --no-cache-dir -r requirements.txt

# Copy Meltano project and installed plugins from builder
COPY --from=meltano-builder /app/.meltano /app/.meltano
COPY --from=meltano-builder /app/meltano.yml ./
COPY --from=meltano-builder /app/transform ./transform

# Copy application code
COPY main.py ./
COPY run.py ./
COPY etl_logger.py ./
COPY utils.py ./
COPY orchestration/ ./orchestration/
COPY api/ ./api/

# Ensure Meltano project is valid
RUN meltano config 2>/dev/null || true

# Port: 8080 for Fly.io (sets PORT), 8001 for local docker-compose override
ENV PORT=8080
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# Start FastAPI (use PORT from env for Fly.io compatibility)
CMD ["sh", "-c", "exec uvicorn main:app --host 0.0.0.0 --port ${PORT}"]
