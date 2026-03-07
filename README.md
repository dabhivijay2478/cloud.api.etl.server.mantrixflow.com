# MANTrixFlow ETL Server — Singer Architecture

Singer-based ETL microservice using `tap-postgres` and `target-postgres` subprocess CLIs.

See [docs/etl-data-flow.md](../../docs/etl-data-flow.md) for the full Incoming Data → Transformation → Outgoing Data pipeline flow.

## Architecture

```
tap-postgres --config --catalog [--state] | singer_transformer | target-postgres --config
```

- **Zero Supabase at ETL level** — all DB access through NestJS API callbacks
- **Zero dlt** — Singer subprocess CLIs only
- **K8s native** — semaphore-controlled concurrency, 503 backpressure, graceful shutdown

## Setup

```bash
# Clone Singer repos
mkdir vendor
git clone https://github.com/MeltanoLabs/tap-postgres.git vendor/tap-postgres
git clone https://github.com/MeltanoLabs/target-postgres.git vendor/target-postgres

# Create venv and install
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -e vendor/tap-postgres -e vendor/target-postgres

# Copy .env (required for tap/target registry; loaded automatically at startup)
cp .env.example .env

# Verify CLIs
tap-postgres --version
target-postgres --version
```

## Run

```bash
source .venv/bin/activate
uvicorn api.main:app --host 0.0.0.0 --port 8000 --workers 1 --loop asyncio
```

> **Note:** Use `--loop asyncio` to avoid uvloop subprocess compatibility issues with Python 3.13 (e.g. `AttributeError: 'StreamReader' object has no attribute 'fileno'`). If using Python 3.12 or earlier with uvloop, you may omit `--loop asyncio`.

## Docker

```bash
docker compose up --build
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET/POST | `/health` | Pod capacity info (active_runs, max_runs, available) |
| POST | `/test-connection` | Spawns `tap-postgres --test` |
| POST | `/discover` | Spawns `tap-postgres --discover`, returns stream list |
| POST | `/preview` | FULL_TABLE tap through transformer, returns N records |
| POST | `/sync` | Async sync — returns `{job_id, status: "accepted"}` or 503 |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TAP_POSTGRES` | (required) | Tap executable for Postgres. Set in .env (see .env.example). Loaded automatically at startup. |
| `TARGET_POSTGRES` | (required) | Target executable for Postgres. Set in .env. Loaded automatically at startup. |
| `SSL_CLOUD_HOST_SUFFIXES` | (optional) | Comma-separated host suffixes that auto-enable SSL. If unset, SSL only when connection has explicit `ssl` config. |
| `MAX_CONCURRENT_RUNS` | 20 | Max Singer chains per pod (semaphore) |
| `MAX_TAPS_PER_SOURCE` | 3 | Max concurrent taps per source host:port |

## K8s Deployment

```bash
kubectl apply -f k8s/deployment.yaml
kubectl apply -f k8s/hpa.yaml
kubectl apply -f k8s/service.yaml
```

HPA scales from 3 pods (60 concurrent) to 30 pods (600 concurrent) based on CPU.

## Project Structure

```
api/
  main.py          — FastAPI app with lifespan (graceful shutdown)
  routes/
    health.py      — GET/POST /health
    test_connection.py
    discover.py
    preview.py
    sync.py        — POST /sync (async, 503 backpressure)
config/
  connections.py   — Postgres connection string builder
core/
  singer_runner.py     — Subprocess lifecycle manager
  singer_transformer.py — Singer message transformer (stdin/stdout)
  catalog_builder.py   — Builds Singer catalog JSON
  config_builder.py    — Builds tap/target config dicts
  concurrency.py       — RunSemaphore + SourceRateLimiter
vendor/
  tap-postgres/    — Cloned MeltanoLabs/tap-postgres
  target-postgres/ — Cloned MeltanoLabs/target-postgres
k8s/
  deployment.yaml  — K8s Deployment (terminationGracePeriod: 600s)
  hpa.yaml         — HPA (3-30 pods, CPU 60%)
  service.yaml     — ClusterIP Service
```
