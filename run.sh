#!/bin/bash

# Python ETL Service Runner
# Starts the FastAPI ETL microservice
# Run ./setup.sh first if you haven't installed dependencies

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Get the directory where the script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Prefer .venv in this project so dependencies (e.g. pendulum for tap-mysql) are used
if [ -f ".venv/bin/activate" ]; then
    echo -e "${GREEN}✓ Activating .venv${NC}"
    set +e
    source .venv/bin/activate
    set -e
fi

echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  Python ETL Service - Starting${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""

# Check if Python is installed - prefer venv python, then Python 3.12 or 3.11
PYTHON_CMD="python3"
if [ -n "${VIRTUAL_ENV:-}" ] && [ -x "${VIRTUAL_ENV}/bin/python" ]; then
    PYTHON_CMD="${VIRTUAL_ENV}/bin/python"
elif command -v python3.12 &> /dev/null; then
    PYTHON_CMD="python3.12"
elif command -v python3.11 &> /dev/null; then
    PYTHON_CMD="python3.11"
elif ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python 3 is not installed. Please install Python 3.11 or 3.12.${NC}"
    exit 1
fi

# Check Python version
PYTHON_VERSION=$($PYTHON_CMD --version 2>&1 | awk '{print $2}')
echo -e "${GREEN}✓ Using Python: $PYTHON_VERSION${NC}"

# Quick check if dependencies are installed
if ! $PYTHON_CMD -c "import fastapi, uvicorn" &> /dev/null; then
    echo -e "${YELLOW}⚠ Dependencies not found. Running setup first...${NC}"
    echo ""
    ./setup.sh
    echo ""
fi

# Load environment variables
if [ -f ".env" ]; then
    echo -e "${GREEN}✓ Loading environment variables from .env${NC}"
    set -a
    source .env
    set +a
else
    echo -e "${YELLOW}⚠ .env file not found. Using defaults.${NC}"
    echo -e "${YELLOW}   Copy .env.example to .env and configure it${NC}"
fi

# Set defaults if not in .env
export PORT=${PORT:-8001}
export LOG_LEVEL=${LOG_LEVEL:-INFO}
export RELOAD=${RELOAD:-false}

echo -e "${GREEN}✓ Configuration:${NC}"
echo -e "  - Port: $PORT"
echo -e "  - Log Level: $LOG_LEVEL"
echo ""

# Check if port is available
if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1 ; then
    echo -e "${RED}❌ Port $PORT is already in use.${NC}"
    echo -e "${YELLOW}   Please stop the service using that port or change PORT in .env${NC}"
    exit 1
fi

# Start the service
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}🚀 Starting FastAPI server...${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${GREEN}   API URL:      http://localhost:$PORT${NC}"
echo -e "${GREEN}   API Docs:     http://localhost:$PORT/docs${NC}"
echo -e "${GREEN}   Health Check: http://localhost:$PORT/health${NC}"
echo ""
echo -e "${YELLOW}Press Ctrl+C to stop the server${NC}"
echo ""

# Run the service
LOG_LEVEL_LOWER=$(echo "$LOG_LEVEL" | tr '[:upper:]' '[:lower:]')
if [ "$RELOAD" = "true" ] || [ "$RELOAD" = "1" ]; then
    exec $PYTHON_CMD -m uvicorn main:app --host 0.0.0.0 --port "$PORT" --log-level "$LOG_LEVEL_LOWER" --reload
else
    exec $PYTHON_CMD -m uvicorn main:app --host 0.0.0.0 --port "$PORT" --log-level "$LOG_LEVEL_LOWER"
fi
