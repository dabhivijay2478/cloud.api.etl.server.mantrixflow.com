#!/bin/bash
# Full ETL pipeline validation: test-connection, discover, collect, delta-check, run-meltano-pipeline, verify destination data
set +e  # Don't exit on first failure; run all tests
ETL="${1:-http://localhost:8001}"
AUTH="Authorization: Bearer test-token"
PG='{"host":"localhost","port":15432,"database":"testdb","username":"testuser","password":"testpass"}'
MYSQL='{"host":"localhost","port":13306,"database":"testdb","username":"testuser","password":"testpass"}'
MONGO='{"connection_string":"mongodb://testuser:testpass@localhost:27018/?authSource=admin","database":"testdb","auth_source":"admin"}'

pass() { echo "  ✅ PASS: $1"; }
fail() { echo "  ❌ FAIL: $1"; }
section() { echo ""; echo "========== $1 =========="; }

section "1. TEST CONNECTION"
r=$(curl -sS -X POST "$ETL/test-connection" -H "$AUTH" -H "Content-Type: application/json" -d "{\"type\":\"postgresql\",\"host\":\"localhost\",\"port\":15432,\"database\":\"testdb\",\"username\":\"testuser\",\"password\":\"testpass\"}")
echo "$r" | grep -q '"success":true' && pass "test-connection postgresql" || fail "test-connection postgresql: $r"
r=$(curl -sS -X POST "$ETL/test-connection" -H "$AUTH" -H "Content-Type: application/json" -d "{\"type\":\"mysql\",\"host\":\"localhost\",\"port\":13306,\"database\":\"testdb\",\"username\":\"testuser\",\"password\":\"testpass\"}")
echo "$r" | grep -q '"success":true' && pass "test-connection mysql" || fail "test-connection mysql: $r"
r=$(curl -sS -X POST "$ETL/test-connection" -H "$AUTH" -H "Content-Type: application/json" -d "{\"type\":\"mongodb\",\"connection_string\":\"mongodb://testuser:testpass@localhost:27018/?authSource=admin\"}")
echo "$r" | grep -q '"success":true' && pass "test-connection mongodb" || fail "test-connection mongodb: $r"

section "2. DISCOVER SCHEMA"
r=$(curl -sS -X POST "$ETL/discover-schema/postgresql" -H "$AUTH" -H "Content-Type: application/json" -d "{\"connection_config\":$PG,\"table_name\":\"test_users\",\"schema_name\":\"public\"}")
echo "$r" | grep -q '"columns"' && pass "discover-schema postgresql" || fail "discover-schema postgresql"
r=$(curl -sS -X POST "$ETL/discover-schema/mysql" -H "$AUTH" -H "Content-Type: application/json" -d "{\"connection_config\":$MYSQL,\"table_name\":\"test_products\"}")
echo "$r" | grep -q '"columns"' && pass "discover-schema mysql" || fail "discover-schema mysql"
r=$(curl -sS -X POST "$ETL/discover-schema/mongodb" -H "$AUTH" -H "Content-Type: application/json" -d "{\"connection_config\":$MONGO,\"table_name\":\"test_customers\"}")
echo "$r" | grep -q '"columns"' && pass "discover-schema mongodb" || fail "discover-schema mongodb"

section "3. COLLECT"
r=$(curl -sS -X POST "$ETL/collect/postgresql" -H "$AUTH" -H "Content-Type: application/json" -d "{\"connection_config\":$PG,\"table_name\":\"test_users\",\"schema_name\":\"public\",\"sync_mode\":\"full\",\"limit\":5}")
echo "$r" | grep -q '"rows"' && pass "collect postgresql" || fail "collect postgresql"
r=$(curl -sS -X POST "$ETL/collect/mysql" -H "$AUTH" -H "Content-Type: application/json" -d "{\"connection_config\":$MYSQL,\"table_name\":\"test_products\",\"sync_mode\":\"full\",\"limit\":5}")
echo "$r" | grep -q '"rows"' && pass "collect mysql" || fail "collect mysql"
r=$(curl -sS -X POST "$ETL/collect/mongodb" -H "$AUTH" -H "Content-Type: application/json" -d "{\"connection_config\":$MONGO,\"table_name\":\"test_customers\",\"sync_mode\":\"full\",\"limit\":5}")
echo "$r" | grep -q '"rows"' && pass "collect mongodb" || fail "collect mongodb"

section "4. DELTA CHECK"
r=$(curl -sS -X POST "$ETL/delta-check/postgresql" -H "$AUTH" -H "Content-Type: application/json" -d "{\"connection_config\":$PG,\"table_name\":\"test_users\",\"schema_name\":\"public\"}")
echo "$r" | grep -q '"has_changes"' && pass "delta-check postgresql" || fail "delta-check postgresql"

section "5. RUN PIPELINE: postgres-to-postgres"
r=$(curl -sS -X POST "$ETL/run-meltano-pipeline" -H "$AUTH" -H "Content-Type: application/json" -d "{
  \"direction\":\"postgres-to-postgres\",
  \"source_connection_config\":$PG,
  \"dest_connection_config\":$PG,
  \"source_table\":\"test_users\",
  \"dest_table\":\"synced_users_pg\",
  \"source_schema\":\"public\",
  \"dest_schema\":\"public\",
  \"sync_mode\":\"full\",
  \"write_mode\":\"replace\"
}")
if echo "$r" | grep -q '"rows_read"'; then
  read=$(echo "$r"|grep -o '"rows_read":[0-9]*'|cut -d: -f2)
  written=$(echo "$r"|grep -o '"rows_written":[0-9]*'|cut -d: -f2)
  pass "postgres-to-postgres (read=$read, written=$written)"
else fail "postgres-to-postgres: $(echo "$r"|head -c300)"; fi

section "6. RUN PIPELINE: mysql-to-postgres"
r=$(curl -sS -X POST "$ETL/run-meltano-pipeline" -H "$AUTH" -H "Content-Type: application/json" -d "{
  \"direction\":\"mysql-to-postgres\",
  \"source_connection_config\":$MYSQL,
  \"dest_connection_config\":$PG,
  \"source_table\":\"test_products\",
  \"dest_table\":\"synced_products\",
  \"sync_mode\":\"full\",
  \"write_mode\":\"replace\"
}")
if echo "$r" | grep -q '"rows_read"'; then
  read=$(echo "$r"|grep -o '"rows_read":[0-9]*'|cut -d: -f2)
  written=$(echo "$r"|grep -o '"rows_written":[0-9]*'|cut -d: -f2)
  pass "mysql-to-postgres (read=$read, written=$written)"
else fail "mysql-to-postgres: $(echo "$r"|head -c300)"; fi

section "7. RUN PIPELINE: mongodb-to-postgres"
r=$(curl -sS -X POST "$ETL/run-meltano-pipeline" -H "$AUTH" -H "Content-Type: application/json" -d "{
  \"direction\":\"mongodb-to-postgres\",
  \"source_connection_config\":$MONGO,
  \"dest_connection_config\":$PG,
  \"source_table\":\"test_customers\",
  \"dest_table\":\"synced_customers\",
  \"sync_mode\":\"full\",
  \"write_mode\":\"replace\"
}")
if echo "$r" | grep -q '"rows_read"'; then
  read=$(echo "$r"|grep -o '"rows_read":[0-9]*'|cut -d: -f2)
  written=$(echo "$r"|grep -o '"rows_written":[0-9]*'|cut -d: -f2)
  pass "mongodb-to-postgres (read=$read, written=$written)"
else fail "mongodb-to-postgres: $(echo "$r"|head -c400)"; fi

section "8. VERIFY DESTINATION DATA (Postgres)"
echo "Checking synced_users_pg, synced_products, synced_customers in testdb..."
PGPASSWORD=testpass psql -h localhost -p 15432 -U testuser -d testdb -t -c "SELECT 'synced_users_pg' as tbl, COUNT(*) FROM synced_users_pg;" 2>/dev/null || echo "  (psql not available)"
PGPASSWORD=testpass psql -h localhost -p 15432 -U testuser -d testdb -t -c "SELECT 'synced_products' as tbl, COUNT(*) FROM synced_products;" 2>/dev/null || echo "  (psql not available)"
PGPASSWORD=testpass psql -h localhost -p 15432 -U testuser -d testdb -t -c "SELECT 'synced_customers' as tbl, COUNT(*) FROM synced_customers;" 2>/dev/null || echo "  (psql not available)"
echo "Source row counts: test_users=20, test_products=15, test_customers=10"

section "9. SUMMARY"
echo "Run complete. Review results above."
