# New ETL (Python FastAPI + dlt)

dlt-based ETL: PostgreSQL and MongoDB sources, PostgreSQL destination.

## Setup

```bash
# Python 3.11
pip install -r requirements.txt
```

**Note:** Credentials come from the API request payload (NestJS fetches from `data_source_connections`). `.dlt/secrets.toml` is a dlt init template; for MANTrixFlow, all syncs are API-driven.

## Start

```bash
# Development (port 8000)
uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

# Production
uvicorn api.main:app --host 0.0.0.0 --port 8000
```

## Docker

```bash
# Docker Compose (recommended)
docker compose up -d

# Or plain Docker
docker build -t new-etl .
docker run -p 8000:8000 new-etl
```

## Conventions

See [CONVENTIONS.md](CONVENTIONS.md) for dlt usage, sync modes, and rules.

## dlt Sources

- [pg_replication](pg_replication/README.md) — Postgres WAL CDC
- [mongodb](mongodb/README.md) — MongoDB collections
