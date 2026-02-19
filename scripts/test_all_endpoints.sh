#!/bin/bash
# Test all ETL endpoints against local Docker databases.
# Prerequisites: docker-compose.test.yml up, seed_test_data.py run, ETL service on :8001
#
# Usage: ./scripts/test_all_endpoints.sh [ETL_BASE_URL]
# Default ETL_BASE_URL: http://localhost:8001

set -e
ETL_BASE_URL="${1:-http://localhost:8001}"
AUTH="Authorization: Bearer test-token"
PG_PORT="${PG_TEST_PORT:-15432}"
MYSQL_PORT="${MYSQL_TEST_PORT:-13306}"
MONGO_PORT="${MONGO_TEST_PORT:-27018}"

echo "Testing ETL endpoints at $ETL_BASE_URL"
echo ""

# --- Health (no auth) ---
test_health() {
  echo "GET /"
  curl -sS "$ETL_BASE_URL/" | head -c 200
  echo ""
  echo "GET /health"
  curl -sS "$ETL_BASE_URL/health" | head -c 200
  echo ""
}

# --- Test Connection ---
test_connection() {
  echo ""
  echo "POST /test-connection (postgresql)"
  curl -sS -X POST "$ETL_BASE_URL/test-connection" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "{\"type\":\"postgresql\",\"host\":\"localhost\",\"port\":$PG_PORT,\"database\":\"testdb\",\"username\":\"testuser\",\"password\":\"testpass\"}" | head -c 300
  echo ""

  echo ""
  echo "POST /test-connection (mysql)"
  curl -sS -X POST "$ETL_BASE_URL/test-connection" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "{\"type\":\"mysql\",\"host\":\"localhost\",\"port\":$MYSQL_PORT,\"database\":\"testdb\",\"username\":\"testuser\",\"password\":\"testpass\"}" | head -c 300
  echo ""

  echo ""
  echo "POST /test-connection (mongodb)"
  curl -sS -X POST "$ETL_BASE_URL/test-connection" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "{\"type\":\"mongodb\",\"connection_string\":\"mongodb://testuser:testpass@localhost:$MONGO_PORT/?authSource=admin\"}" | head -c 300
  echo ""
}

# --- Discover Schema ---
test_discover() {
  echo ""
  echo "POST /discover-schema/postgresql"
  curl -sS -X POST "$ETL_BASE_URL/discover-schema/postgresql" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "{\"connection_config\":{\"host\":\"localhost\",\"port\":$PG_PORT,\"database\":\"testdb\",\"username\":\"testuser\",\"password\":\"testpass\"},\"table_name\":\"test_users\",\"schema_name\":\"public\"}" | head -c 500
  echo ""

  echo ""
  echo "POST /discover-schema/mysql"
  curl -sS -X POST "$ETL_BASE_URL/discover-schema/mysql" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "{\"connection_config\":{\"host\":\"localhost\",\"port\":$MYSQL_PORT,\"database\":\"testdb\",\"username\":\"testuser\",\"password\":\"testpass\"},\"table_name\":\"test_products\"}" | head -c 500
  echo ""

  echo ""
  echo "POST /discover-schema/mongodb"
  curl -sS -X POST "$ETL_BASE_URL/discover-schema/mongodb" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "{\"connection_config\":{\"connection_string\":\"mongodb://testuser:testpass@localhost:$MONGO_PORT/?authSource=admin\",\"database\":\"testdb\",\"auth_source\":\"admin\"},\"table_name\":\"test_customers\"}" | head -c 500
  echo ""
}

# --- Collect ---
test_collect() {
  echo ""
  echo "POST /collect/postgresql"
  curl -sS -X POST "$ETL_BASE_URL/collect/postgresql" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "{\"connection_config\":{\"host\":\"localhost\",\"port\":$PG_PORT,\"database\":\"testdb\",\"username\":\"testuser\",\"password\":\"testpass\"},\"table_name\":\"test_users\",\"schema_name\":\"public\",\"sync_mode\":\"full\",\"limit\":3}" | head -c 600
  echo ""

  echo ""
  echo "POST /collect/mysql"
  curl -sS -X POST "$ETL_BASE_URL/collect/mysql" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "{\"connection_config\":{\"host\":\"localhost\",\"port\":$MYSQL_PORT,\"database\":\"testdb\",\"username\":\"testuser\",\"password\":\"testpass\"},\"table_name\":\"test_products\",\"sync_mode\":\"full\",\"limit\":2}" | head -c 600
  echo ""

  echo ""
  echo "POST /collect/mongodb"
  curl -sS -X POST "$ETL_BASE_URL/collect/mongodb" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "{\"connection_config\":{\"connection_string\":\"mongodb://testuser:testpass@localhost:$MONGO_PORT/?authSource=admin\",\"database\":\"testdb\",\"auth_source\":\"admin\"},\"table_name\":\"test_customers\",\"sync_mode\":\"full\",\"limit\":2}" | head -c 600
  echo ""
}

# --- Delta Check ---
test_delta_check() {
  echo ""
  echo "POST /delta-check/postgresql"
  curl -sS -X POST "$ETL_BASE_URL/delta-check/postgresql" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "{\"connection_config\":{\"host\":\"localhost\",\"port\":$PG_PORT,\"database\":\"testdb\",\"username\":\"testuser\",\"password\":\"testpass\"},\"table_name\":\"test_users\",\"schema_name\":\"public\"}" | head -c 300
  echo ""
}

# --- Run Meltano Pipeline (mongodb-to-postgres) ---
test_pipeline() {
  echo ""
  echo "POST /run-meltano-pipeline (mongodb-to-postgres)"
  curl -sS -X POST "$ETL_BASE_URL/run-meltano-pipeline" \
    -H "$AUTH" -H "Content-Type: application/json" \
    -d "{
      \"direction\":\"mongodb-to-postgres\",
      \"source_connection_config\":{\"connection_string\":\"mongodb://testuser:testpass@localhost:$MONGO_PORT/?authSource=admin\",\"database\":\"testdb\",\"auth_source\":\"admin\"},
      \"dest_connection_config\":{\"host\":\"localhost\",\"port\":$PG_PORT,\"database\":\"testdb\",\"username\":\"testuser\",\"password\":\"testpass\"},
      \"source_table\":\"test_customers\",
      \"dest_table\":\"synced_customers\",
      \"sync_mode\":\"full\",
      \"write_mode\":\"replace\"
    }" | head -c 500
  echo ""
}

# --- dbt-models (auth required) ---
test_dbt_models() {
  echo ""
  echo "GET /dbt-models"
  curl -sS "$ETL_BASE_URL/dbt-models" -H "$AUTH" | head -c 200
  echo ""
}

# --- Auth rejection ---
test_auth_rejection() {
  echo ""
  echo "POST /test-connection without auth (expect 401)"
  curl -sS -w "\nHTTP_CODE:%{http_code}" -X POST "$ETL_BASE_URL/test-connection" \
    -H "Content-Type: application/json" \
    -d "{\"type\":\"postgresql\"}" | tail -1
  echo ""
}

test_health
test_connection
test_discover
test_collect
test_delta_check
test_pipeline
test_dbt_models
test_auth_rejection

echo ""
echo "Done. Check output above for success/errors."
