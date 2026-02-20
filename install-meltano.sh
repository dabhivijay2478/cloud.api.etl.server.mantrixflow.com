#!/bin/bash
# Install Meltano plugins including target-mysql and target-mongodb.
# Fixes: mysqlclient (MySQL client libs) and target-mongodb (six.moves build error).
# Run from apps/etl: ./install-meltano.sh

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  Meltano Plugin Installation${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo ""

# 1. Install MySQL client libs for target-mysql (macOS)
if [[ "$OSTYPE" == "darwin"* ]]; then
    if ! brew list mysql-client &>/dev/null && ! brew list mysql &>/dev/null; then
        echo -e "${YELLOW}Installing mysql-client and pkg-config for target-mysql...${NC}"
        brew install mysql-client pkg-config
        echo -e "${GREEN}✓ mysql-client installed${NC}"
    else
        echo -e "${GREEN}✓ mysql-client already installed${NC}"
    fi
    # Set PKG_CONFIG_PATH so mysqlclient can find MySQL libs
    if [[ -d /opt/homebrew/opt/mysql-client ]]; then
        export PKG_CONFIG_PATH="/opt/homebrew/opt/mysql-client/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
    elif [[ -d /usr/local/opt/mysql-client ]]; then
        export PKG_CONFIG_PATH="/usr/local/opt/mysql-client/lib/pkgconfig${PKG_CONFIG_PATH:+:$PKG_CONFIG_PATH}"
    fi
fi

# 2. Upgrade psutil (macOS + Python 3.12 cpu_count bug)
python3 -m pip install --upgrade "psutil>=6.0.0" --quiet 2>/dev/null || true

# 3. Run meltano install
echo -e "${YELLOW}Running meltano install...${NC}"
if meltano install 2>&1 | tee /tmp/meltano-install.log; then
    echo -e "${GREEN}✓ All Meltano plugins installed${NC}"
else
    echo -e "${YELLOW}Meltano install had some failures. Attempting fixes...${NC}"
fi

# 4. Fix target-mongodb if it failed (six.moves / poetry build error)
TARGET_MONGO_VENV="$SCRIPT_DIR/.meltano/loaders/target-mongodb/venv"
TARGET_MONGO_BIN="$TARGET_MONGO_VENV/bin/target-mongodb"
if [[ ! -x "$TARGET_MONGO_BIN" ]] && [[ -d "$TARGET_MONGO_VENV" ]]; then
    echo -e "${YELLOW}Fixing target-mongodb (six.moves build error)...${NC}"
    uv pip install --python "$TARGET_MONGO_VENV/bin/python" six poetry-core
    uv pip install --python "$TARGET_MONGO_VENV/bin/python" --no-build-isolation "git+https://gitlab.com/hotglue/target-mongodb.git" || true
    if [[ -x "$TARGET_MONGO_BIN" ]]; then
        echo -e "${GREEN}✓ target-mongodb installed (manual fix)${NC}"
    else
        echo -e "${YELLOW}⚠ target-mongodb fix failed. Tests requiring it will be skipped.${NC}"
    fi
fi

# 5. Fix target-mysql if it failed (pendulum/distutils, pkg_resources, pymysql)
TARGET_MYSQL_VENV="$SCRIPT_DIR/.meltano/loaders/target-mysql/venv"
TARGET_MYSQL_BIN="$TARGET_MYSQL_VENV/bin/target-mysql"
if [[ ! -x "$TARGET_MYSQL_BIN" ]] && [[ -d "$TARGET_MYSQL_VENV" ]]; then
    echo -e "${YELLOW}Fixing target-mysql (manual install)...${NC}"
    uv pip install --python "$TARGET_MYSQL_VENV/bin/python" setuptools poetry-core "setuptools<82" pymysql
    uv pip install --python "$TARGET_MYSQL_VENV/bin/python" --no-build-isolation thk-target-mysql 2>/dev/null || true
fi
if [[ -x "$TARGET_MYSQL_BIN" ]]; then
    # Ensure setuptools<82 (pkg_resources) and pymysql for mysql+pymysql:// URLs
    uv pip install --python "$TARGET_MYSQL_VENV/bin/python" "setuptools<82" pymysql 2>/dev/null || true
fi

# Note: mongodb-to-mysql may fail with "Specified key was too long" when test_customers
# replication_key (VARCHAR 1000) exceeds MySQL utf8mb4 key limit (3072 bytes). Known limitation.

echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}Meltano install complete. Run: pytest tests/ -v${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════${NC}"
