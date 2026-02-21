#!/bin/bash

# Python ETL Service Setup Script
# Installs all dependencies and Singer taps
# Run this once before using run.sh

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

echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  Python ETL Service - Setup${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""

# Check if Python is installed - prefer Python 3.12 or 3.11
PYTHON_CMD="python3"
if command -v python3.12 &> /dev/null; then
    PYTHON_CMD="python3.12"
    echo -e "${GREEN}✓ Found Python 3.12 (recommended)${NC}"
elif command -v python3.11 &> /dev/null; then
    PYTHON_CMD="python3.11"
    echo -e "${GREEN}✓ Found Python 3.11 (recommended)${NC}"
elif ! command -v python3 &> /dev/null; then
    echo -e "${RED}❌ Python 3 is not installed. Please install Python 3.11 or 3.12.${NC}"
    echo -e "${YELLOW}   Install with: brew install python@3.12${NC}"
    exit 1
fi

# Check Python version
PYTHON_VERSION=$($PYTHON_CMD --version 2>&1 | awk '{print $2}')
PYTHON_MAJOR=$(echo $PYTHON_VERSION | cut -d. -f1)
PYTHON_MINOR=$(echo $PYTHON_VERSION | cut -d. -f2)

echo -e "${GREEN}✓ Using Python version: $PYTHON_VERSION${NC}"

# Require Python 3.11 or 3.12 (3.13 breaks pydantic-core build and many wheels)
if [ "$PYTHON_MAJOR" = "3" ] && [ "$PYTHON_MINOR" = "13" ]; then
    echo -e "${RED}❌ Python 3.13 is not supported (pydantic-core and other deps lack wheels).${NC}"
    echo -e "${YELLOW}   Use Python 3.12: brew install python@3.12, then run this script again.${NC}"
    exit 1
fi

# Create or fix .venv: use Python 3.11/3.12 only (run.sh will use this venv)
if [ -f ".venv/bin/python" ]; then
    VENV_VER=$(.venv/bin/python --version 2>&1 | awk '{print $2}')
    VENV_MINOR=$(echo "$VENV_VER" | cut -d. -f2)
    if [ "$VENV_MINOR" = "13" ]; then
        echo -e "${YELLOW}⚠ Removing existing .venv (Python 3.13) and recreating with Python $PYTHON_VERSION${NC}"
        rm -rf .venv
    fi
fi
if [ ! -f ".venv/bin/activate" ]; then
    echo -e "${BLUE}Creating .venv with $PYTHON_CMD...${NC}"
    $PYTHON_CMD -m venv .venv
    echo -e "${GREEN}✓ .venv created${NC}"
fi
if [ -f ".venv/bin/activate" ]; then
    echo -e "${GREEN}✓ Activating .venv for installs${NC}"
    set +e
    source .venv/bin/activate
    set -e
    PYTHON_CMD=".venv/bin/python"
fi

# Check if uv is installed
if ! command -v uv &> /dev/null; then
    echo -e "${YELLOW}📦 uv is not installed. Installing uv...${NC}"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    export PATH="$HOME/.cargo/bin:$PATH"
    
    # Verify installation
    if ! command -v uv &> /dev/null; then
        echo -e "${RED}❌ Failed to install uv. Please install manually:${NC}"
        echo -e "${YELLOW}   curl -LsSf https://astral.sh/uv/install.sh | sh${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ uv installed successfully${NC}"
fi

echo -e "${GREEN}✓ uv version: $(uv --version)${NC}"
echo ""

# Step 1: Install Python dependencies (into .venv when present)
echo -e "${BLUE}Step 1: Installing Python dependencies...${NC}"
if [ -n "${VIRTUAL_ENV:-}" ] || [ -f ".venv/bin/python" ]; then
    PIP_CMD="${PYTHON_CMD} -m pip"
    if ! $PYTHON_CMD -m pip --version &> /dev/null; then
        $PYTHON_CMD -m ensurepip --upgrade 2>/dev/null || true
    fi
    $PIP_CMD install --upgrade pip
    $PIP_CMD install -r requirements.txt
    echo -e "${GREEN}✓ Dependencies installed into .venv (includes pendulum for tap-mysql)${NC}"
elif uv pip install -r requirements.txt 2>/dev/null; then
    echo -e "${GREEN}✓ Dependencies installed with uv${NC}"
else
    echo -e "${YELLOW}⚠ Trying pip...${NC}"
    if ! $PYTHON_CMD -m pip --version &> /dev/null; then
        $PYTHON_CMD -m ensurepip --upgrade 2>/dev/null || true
    fi
    $PYTHON_CMD -m pip install --upgrade pip
    $PYTHON_CMD -m pip install -r requirements.txt
    echo -e "${GREEN}✓ Dependencies installed with pip${NC}"
fi
echo ""

# Step 2: Meltano (taps and targets - required for pipelines)
# Skip with: SKIP_MELTANO=1 ./setup.sh
echo -e "${BLUE}Step 2: Meltano (taps and targets)...${NC}"
if [ "${SKIP_MELTANO:-0}" = "1" ]; then
    echo -e "${YELLOW}  Skipping Meltano (SKIP_MELTANO=1). Pipelines require meltano install.${NC}"
elif [ -f "meltano.yml" ]; then
    if command -v meltano &> /dev/null; then
        echo -e "${YELLOW}📦 Installing Meltano plugins (tap-postgres, tap-mongodb, target-postgres)...${NC}"
        echo -e "${YELLOW}   This may take 2–5 minutes on first run.${NC}"
        # Upgrade psutil to avoid macOS + Python 3.12 bug (cpu_count_logical SystemError)
        python3 -m pip install --upgrade "psutil>=6.0.0" --quiet 2>/dev/null || true
        if meltano install; then
            echo -e "${GREEN}✓ Meltano plugins installed${NC}"
        else
            echo -e "${YELLOW}⚠ Meltano install had issues. Try: python3 -m pip install --upgrade 'psutil>=6.0.0' && meltano install${NC}"
            echo -e "${YELLOW}   Or skip with SKIP_MELTANO=1 ./setup.sh${NC}"
        fi
    else
        echo -e "${YELLOW}  Meltano not found. For static CLI mode, install with: pipx install meltano${NC}"
        echo -e "${YELLOW}  Dynamic mode (POST /run-meltano-pipeline) works without Meltano.${NC}"
    fi
else
    echo -e "${GREEN}✓ meltano.yml not present, skipping Meltano${NC}"
fi
echo ""

echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}✅ Setup completed successfully!${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "${GREEN}Next steps:${NC}"
echo -e "  1. Configure your .env file (copy from .env.example if needed)"
echo -e "  2. Run ./run.sh to start the ETL service"
echo -e "  3. For Meltano CLI: copy .env.meltano.example to .env and set POSTGRES_URL, MYSQL_URL, MONGODB_URI"
echo ""
