# MANTrixFlow ETL Server — dlt Architecture

Python FastAPI ETL service powered by `dlt`, SQLAlchemy reflection, and first-party copies of the dlt verified MongoDB and PostgreSQL replication sources.

## Architecture

```text
NestJS queue -> FastAPI /sync -> core/dlt_runner.py -> dlt source/resource -> dlt destination
```

- One execution engine for syncs, previews, and transforms
- No Singer subprocesses or vendor tap/target repos
- SQL discovery via SQLAlchemy, Mongo discovery via sampled document inference
- Async dispatch with callback/state handoff back to NestJS

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Run

```bash
source .venv/bin/activate
uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 1 --loop asyncio
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET/POST | `/health` | Pod capacity info |
| POST | `/test-connection` | SQLAlchemy ping or MongoDB ping |
| POST | `/discover` | SQLAlchemy reflection or MongoDB schema sampling |
| POST | `/preview` | Reads sample records through dlt resources, no destination write |
| POST | `/sync` | Async dlt sync, returns `{job_id, status: "accepted"}` or 503 |
| POST | `/cdc/verify` | PostgreSQL logical replication checks |
| POST | `/cleanup/connection` | Drops PostgreSQL replication slot when needed |

## Environment

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_CONCURRENT_RUNS` | 20 | Max concurrent sync tasks per pod |
| `MAX_TAPS_PER_SOURCE` | 3 | Max concurrent syncs per source host:port |

## Project Structure

```text
api/
  main.py
  routes/
    health.py
    test_connection.py
    discover.py
    preview.py
    sync.py
core/
  dlt_runner.py
  connection_utils.py
  transform_utils.py
  concurrency.py
mongodb/
  __init__.py
  helpers.py
pg_replication/
  __init__.py
  helpers.py
  decoders.py
  schema_types.py
```
