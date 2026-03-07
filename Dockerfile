# MANTrixFlow ETL Server — Singer tap-postgres + target-postgres
# Python 3.13, vendor repos installed at build time

FROM python:3.13-slim

WORKDIR /app

# Install system deps for psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for layer caching
COPY requirements.txt .

# Install Python deps
RUN pip install --no-cache-dir -r requirements.txt

# Clone and install Singer vendor repos (required for tap/target CLIs)
RUN mkdir -p vendor \
    && git clone --depth 1 https://github.com/MeltanoLabs/tap-postgres.git vendor/tap-postgres \
    && git clone --depth 1 https://github.com/MeltanoLabs/target-postgres.git vendor/target-postgres \
    && pip install --no-cache-dir -e vendor/tap-postgres -e vendor/target-postgres

# Copy application code
COPY api/ api/
COPY core/ core/

# Verify tap/target executables
RUN tap-postgres --version && target-postgres --version

ENV TAP_POSTGRES=tap-postgres TARGET_POSTGRES=target-postgres

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1", "--loop", "asyncio"]
