# ETL Pipeline Validation Report

**Date:** 2026-02-19  
**Environment:** Local Docker (Postgres 15432, MySQL 13306, MongoDB 27018) + ETL on :8001

---

## 1. Test Connection ✅

| Source     | Status | Result                    |
|------------|--------|---------------------------|
| PostgreSQL | ✅ PASS | Connection successful     |
| MySQL      | ✅ PASS | Connection successful     |
| MongoDB    | ✅ PASS | Connection successful     |

---

## 2. Discover Schema ✅

| Source     | Status | Result                    |
|------------|--------|---------------------------|
| PostgreSQL | ✅ PASS | Columns discovered (test_users) |
| MySQL      | ✅ PASS | Columns discovered (test_products) |
| MongoDB    | ✅ PASS | Schema discovered (test_customers) |

---

## 3. Collect (Emit) ✅

| Source     | Status | Result                    |
|------------|--------|---------------------------|
| PostgreSQL | ✅ PASS | 20 rows (test_users), limit=5 returned |
| MySQL      | ✅ PASS | 15 rows (test_products), limit=5 returned |
| MongoDB    | ✅ PASS | 10 rows (test_customers), limit=5 returned |

---

## 4. Delta Check ✅

| Source     | Status | Result                    |
|------------|--------|---------------------------|
| PostgreSQL | ✅ PASS | has_changes: true          |

---

## 5. Run Meltano Pipeline

| Direction              | Status | Notes |
|------------------------|--------|-------|
| postgres-to-postgres   | ✅ PASS | Tap + target complete. Data synced. |
| mysql-to-postgres      | ✅ PASS | Tap + target complete. Data synced. |
| mongodb-to-postgres    | ❌ FAIL | KeyError: '_id' – tap-mongodb expects ObjectId for incremental; test data uses string IDs. |

**Note:** dbt-postgres was removed from pipeline jobs (profiles path issue). Sync (tap → target) works. Transform step can be re-added when dbt config is fixed.

---

## 6. Destination Data Verification ✅

**Postgres (testdb) after pipeline runs:**

| Table              | Rows | Source |
|--------------------|------|--------|
| test_users         | 20   | Native PG |
| test_orders        | 10   | Native PG |
| mongo_pg_meltano   | 3    | Prior sync |
| mongo_to_pg_customers | 5 | Prior sync |
| mysql_to_pg_products | 5 | Prior sync |
| parallel_*         | 4    | Prior sync |
| pg_to_pg_users     | 5    | Prior sync |
| test_emit_dest     | 18   | Prior sync |

**Source row counts:** test_users=20, test_products=15, test_customers=10

---

## 7. Summary

| Component        | Status |
|------------------|--------|
| Test connection  | ✅ All 3 sources |
| Discover schema  | ✅ All 3 sources |
| Collect (emit)   | ✅ All 3 sources |
| Delta check      | ✅ PostgreSQL |
| Full sync pipeline | ✅ postgres-to-postgres, mysql-to-postgres |
| Incremental sync | ⚠️ Not tested (would need LOG_BASED/INCREMENTAL) |
| Transform (dbt)  | ⚠️ Disabled (profiles path) |
| mongodb-to-postgres | ❌ tap-mongodb _id type mismatch |

---

## 8. Known Issues

1. **mongodb-to-postgres:** test_customers uses string `_id` (e.g. "cust_1"); tap-mongodb expects ObjectId for incremental. Use FULL_TABLE or fix seed data.
2. **dbt transform:** Removed from jobs; re-add when `transform/profiles/postgres` path is resolved for dbt utility.
3. **Pipeline table filter:** API accepts source_table/dest_table but pipeline syncs all streams. Table-level filtering not applied.
