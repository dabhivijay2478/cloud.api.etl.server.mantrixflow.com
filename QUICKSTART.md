# Quick Start Guide

Get the Python ETL service up and running in 3 steps.

## Step 1: Install Dependencies

```bash
cd apps/etl
pip install -r requirements.txt
```

## Step 2: Install Singer Taps

```bash
# Install all taps
pip install -e connectors/tap-postgres
pip install -e connectors/tap-mysql
pip install -e connectors/tap-mongodb
```

## Step 3: Setup Dependencies (First Time Only)

```bash
chmod +x setup.sh
./setup.sh
```

This installs all dependencies and fixes compatibility issues.

## Step 4: Run the Service

```bash
chmod +x run.sh
./run.sh
```

### Alternative: Manual Start

```bash
python3.12 main.py
# or
uvicorn main:app --host 0.0.0.0 --port 8001 --reload
```

## Verify It's Working

Open your browser and visit:
- **Health Check**: http://localhost:8001/health
- **API Docs**: http://localhost:8001/docs

You should see the FastAPI interactive documentation.

## Test Connection

```bash
curl -X POST http://localhost:8001/test-connection \
  -H "Content-Type: application/json" \
  -d '{
    "type": "postgresql",
    "host": "localhost",
    "port": 5432,
    "database": "testdb",
    "username": "user",
    "password": "password"
  }'
```

## Troubleshooting

**Port already in use?**
- Change `PORT=8002` in `.env` file

**Import errors?**
- Make sure you installed the Singer taps: `pip install -e connectors/tap-postgres`

**Connection errors?**
- Verify your database credentials
- Check if database server is running

For more details, see [README.md](./README.md).
