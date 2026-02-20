# Python ETL Service

FastAPI microservice for data extraction, transformation, and loading (ETL) operations.

**Vercel + Meltano**: See [VERCEL_DEPLOYMENT.md](../../VERCEL_DEPLOYMENT.md) for deploying ETL, API (NestJS), and Meltano pipelines to Vercel.

## Features

- **Data Collection**: Extract data from PostgreSQL, MySQL, and MongoDB using Singer taps
- **Custom Transformations**: Execute user-provided Python scripts for data transformation
- **Data Emission**: Load transformed data to destinations (PostgreSQL, MongoDB)
- **Schema Discovery**: Discover table schemas from data sources
- **Incremental Sync**: Support for incremental data synchronization with state/bookmark tracking
- **Connection Testing**: Test database connections before use

## Prerequisites

- **Python 3.11 or 3.12** (recommended for best compatibility)
- Python 3.13 may have issues with `psycopg2-binary` - see [INSTALL_PYTHON312.md](./INSTALL_PYTHON312.md)
- uv (recommended) or pip (Python package manager)
- PostgreSQL, MySQL, or MongoDB (depending on your data sources)

**Important**: `psycopg2-binary` doesn't fully support Python 3.13 yet. For best results, use Python 3.11 or 3.12.

## Installation

### 1. Install Python Dependencies

**Option A: Using uv (Recommended - Faster)**
```bash
cd apps/etl
uv pip install -r requirements.txt
```

**Option B: Using pip**
```bash
cd apps/etl
pip install -r requirements.txt
```

**Note**: If `psycopg2-binary` fails to build on Python 3.13, use Python 3.11 or 3.12.

**Recommended**: Use the project virtual environment so ETL dependencies don't conflict with other tools (e.g. meltano, singer-sdk). Run `./setup.sh` once—it creates `.venv` and installs everything—or create and activate a venv before `pip install -r requirements.txt`. If you see dependency conflicts with `meltano` / `jsonschema` / `singer-sdk`, install inside this project's venv.

### 2. Install Singer Taps

The service uses Singer taps for data extraction. Install them from the local connectors directory:

**Using uv:**
```bash
uv pip install -e connectors/tap-postgres
uv pip install -e connectors/tap-mysql
uv pip install -e connectors/tap-mongodb
```

**Using pip:**
```bash
pip install -e connectors/tap-postgres
pip install -e connectors/tap-mysql
pip install -e connectors/tap-mongodb
```

### 3. Configure Environment

Copy the example environment file and configure it:

```bash
cp .env.example .env
```

Edit `.env` and set:
- `PORT`: Port for the FastAPI server (default: 8001)
- `LOG_LEVEL`: Logging level (default: INFO)
- `SUPABASE_JWT_SECRET`: (Optional) For JWT validation in production

## Running the Service

### Step 1: Setup (First Time Only)

Run the setup script to install all dependencies:

```bash
# Make executable (first time only)
chmod +x setup.sh

# Run setup
./setup.sh
```

This will:
- Check Python version (prefers 3.12 or 3.11)
- Install/check for `uv` (fast Python package manager)
- Install all Python dependencies
- Fix tap-postgres to use `psycopg2-binary` (compatible version)
- Install all Singer taps (tap-postgres, tap-mysql, tap-mongodb)

### Step 2: Run the Service

After setup, run the service:

```bash
# Make executable (first time only)
chmod +x run.sh

# Run the service
./run.sh
```

The run script will:
- Check if dependencies are installed (runs setup.sh if needed)
- Load environment variables from `.env`
- Check if port is available
- Start the FastAPI server

### Option 2: Using Python Runner (Cross-platform)

```bash
python3 run.py
```

This Python script does the same as `run.sh` but works on Windows too.

### Option 3: Using Python Directly

```bash
python main.py
```

### Option 4: Using uvicorn

```bash
uvicorn main:app --host 0.0.0.0 --port 8001 --reload
```

## API Endpoints

### Health Check
- `GET /` - Service status
- `GET /health` - Health check

### Schema Discovery
- `POST /discover-schema/{sourceType}` - Discover schema from a data source
  - Supported types: `postgresql`, `mysql`, `mongodb`

### Data Collection
- `POST /collect/{sourceType}` - Collect data from a source
  - Supports `full` and `incremental` sync modes
  - Returns records and updated checkpoint/state

### Connection Testing
- `POST /test-connection` - Test database connection
  - Validates connection configuration
  - Returns connection status and version info

### Delta Check
- `POST /delta-check/{sourceType}` - Check for changes (incremental sync polling)

## Example Usage

### Test Connection

```bash
curl -X POST http://localhost:8001/test-connection \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "type": "postgresql",
    "host": "localhost",
    "port": 5432,
    "database": "mydb",
    "username": "user",
    "password": "password"
  }'
```

### Discover Schema

```bash
curl -X POST http://localhost:8001/discover-schema/postgresql \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "source_type": "postgresql",
    "connection_config": {
      "host": "localhost",
      "port": 5432,
      "database": "mydb",
      "username": "user",
      "password": "password"
    },
    "table_name": "users",
    "schema_name": "public"
  }'
```

## Run Meltano Pipeline

Use `POST /run-meltano-pipeline` for end-to-end data movement (extract, optional transform via dbt, load). Supports all 9 directions: postgres/mysql/mongodb → postgres/mysql/mongodb (including same-type: postgres-to-postgres, mysql-to-mysql, mongodb-to-mongodb).

## Development

### Running in Development Mode

The service runs with auto-reload by default when using `python main.py` or `uvicorn` with `--reload`.

### Testing

```bash
# Test the service is running
curl http://localhost:8001/health

# Test with authentication
curl http://localhost:8001/ \
  -H "Authorization: Bearer YOUR_JWT_TOKEN"
```

### Logging

Logs are output to stdout. Set `LOG_LEVEL` in `.env` to control verbosity:
- `DEBUG`: Detailed debug information
- `INFO`: General information (default)
- `WARNING`: Warning messages
- `ERROR`: Error messages only

## Troubleshooting

### Import Errors for Singer Taps

If you see errors like "tap-postgres not installed":
```bash
pip install -e connectors/tap-postgres
pip install -e connectors/tap-mysql
pip install -e connectors/tap-mongodb
```

### Port Already in Use

Change the port in `.env`:
```env
PORT=8002
```

### Connection Errors

- Verify database credentials in connection config
- Check network connectivity to database
- Ensure database server is running
- For PostgreSQL, check SSL settings if required

## Meltano: Bidirectional Postgres ↔ MongoDB Sync

### Dynamic Mode (Recommended: connections from database)

All connections are stored in the `data_source_connections` table. When a pipeline runs, the API fetches connection configs from the database and passes them to the ETL. **No static env vars or meltano.yml config needed at runtime.**

**Endpoint:** `POST /run-meltano-pipeline`

```bash
curl -X POST http://localhost:8001/run-meltano-pipeline \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT" \
  -d '{
    "direction": "postgres-to-mongodb",
    "source_connection_config": {"host": "...", "port": 5432, "database": "mydb", "username": "...", "password": "..."},
    "dest_connection_config": {"connection_string": "mongodb://...", "database": "mydb"},
    "source_table": "users",
    "dest_table": "users",
    "sync_mode": "incremental",
    "write_mode": "upsert"
  }'
```

The API (`PythonETLService.runMeltanoPipeline`) gets `sourceConnectionConfig` and `destConnectionConfig` from `getDecryptedConnection()` and passes them to this endpoint.

### Static Mode (CLI/cron with env vars)

A `meltano.yml` is provided for **mongodb-to-postgres** when using static env-based config. The `target-mongodb` loader fails on Python 3.12 (pendulum/distutils), so **postgres-to-mongodb** uses the dynamic API only.

```bash
cd apps/etl
cp .env.meltano.example .env
# Edit .env: POSTGRES_URL, MONGODB_URI, MONGODB_DATABASE
meltano install
meltano run mongodb-to-postgres   # MongoDB → Postgres
```

For **postgres-to-mongodb**, use `POST /run-meltano-pipeline` (dynamic mode).

### Supported Directions

| Direction | Source | Destination | Static Meltano CLI | Dynamic API |
|-----------|--------|-------------|--------------------|-------------|
| `postgres-to-mongodb` | PostgreSQL | MongoDB | — (use dynamic API) | ✓ |
| `mongodb-to-postgres` | MongoDB | PostgreSQL | ✓ | ✓ |

### Bidirectional Protection

To avoid infinite loops when both directions run, use a `transform_script` that adds `_last_modified_by` so each side can filter out records it didn't originate.

### PostgreSQL Incremental Sync (LOG_BASED)

For PostgreSQL sources, incremental sync uses **LOG_BASED** replication (transaction logs via wal2json) instead of replication keys. Requirements (no data is modified in your database by this app):

- **Logical replication enabled** on the source (e.g. Neon: Settings → Beta → Enable logical replication; see [Neon wal2json docs](https://neon.com/docs/extensions/wal2json))
- **Replication slot** `stitch_{dbname}` created by you in your database: `SELECT * FROM pg_create_logical_replication_slot('stitch_yourdb', 'wal2json');`

If logical replication is not enabled or the slot is missing, the pipeline will fail with a clear error message in the UI explaining what to do.

---

## Architecture

- **FastAPI**: Web framework for API endpoints
- **Singer Taps**: Data extraction (tap-postgres, tap-mysql, tap-mongodb)
- **Meltano**: Orchestration for bidirectional Postgres ↔ MongoDB sync (optional)
- **Safe Execution**: Restricted globals for transform script execution
- **State Management**: Singer bookmarks for incremental sync tracking

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `PORT` | Server port | `8001` |
| `LOG_LEVEL` | Logging level | `INFO` |
| `SUPABASE_JWT_SECRET` | JWT secret for auth validation | (optional) |
| `DATABASE_URL` | PostgreSQL URL (if needed) | (optional) |

## Next Steps

1. Start the service: `./run.sh`
2. Test connection: Use `/test-connection` endpoint
3. Discover schema: Use `/discover-schema/{sourceType}` endpoint
4. Create pipeline: Use frontend to configure and run pipelines
