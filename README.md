# Python ETL Service

FastAPI microservice for data extraction, transformation, and loading (ETL) operations.

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

### Data Transformation
- `POST /transform` - Transform records using custom Python script
  - Accepts `transform_script` (Python code) and `rows` (data)
  - Script must define `transform(record)` function

### Data Emission
- `POST /emit/{destType}` - Emit transformed data to destination
  - Supported types: `postgresql`, `mongodb`
  - Supports `append`, `upsert`, and `replace` write modes

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

### Transform Data

```bash
curl -X POST http://localhost:8001/transform \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_JWT_TOKEN" \
  -d '{
    "rows": [{"id": 1, "name": "John"}],
    "transform_script": "def transform(record):\n    return {\"id\": record.get(\"id\"), \"name\": record.get(\"name\")}"
  }'
```

## Transform Script Format

The transform script must define a function named `transform` that accepts a `record` parameter:

```python
import json

def transform(record):
    """
    Transform source record to destination format.
    Use record.get("source_field") to read from source.
    Return dict with destination keys.
    """
    return {
        "id": record.get("id"),
        "name": record.get("name"),
        "email": record.get("email"),
    }
```

**Rules:**
- Function must be named `transform`
- Accepts `record` parameter (dict)
- Returns a dict with destination field names as keys
- Use `record.get("field_name")` to access source fields
- `json` module is available for JSON parsing

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

## Architecture

- **FastAPI**: Web framework for API endpoints
- **Singer Taps**: Data extraction (tap-postgres, tap-mysql, tap-mongodb)
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
