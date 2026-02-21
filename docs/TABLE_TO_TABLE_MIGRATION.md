# MANTrixFlow Table-to-Table Migration

**Meltano + Singer SDK** | **Manual Column Mapping** | **Any Database to Any Database**

## 1. What This Does

MANTrixFlow table-to-table migration moves data from table A in any supported database to table B in any supported database. It requires only Meltano and a Singer tap/target pair — no extra infrastructure.

Column names do not need to match between source and destination. You define the mapping manually: each source column maps to exactly one destination column, and the destination column can have a completely different name. Only columns you list are extracted — everything else is ignored.

## 2. Supported Databases

| Database   | Singer Tap (Source) | Singer Target (Destination) |
|------------|---------------------|-----------------------------|
| PostgreSQL | tap-postgres        | target-postgres             |
| MySQL      | tap-mysql           | target-mysql                |
| MongoDB    | tap-mongodb         | target-mongodb              |
| Snowflake  | tap-snowflake       | target-snowflake            |
| BigQuery   | tap-bigquery        | target-bigquery             |
| SQLite     | tap-sqlite          | target-sqlite               |

## 3. How Column Mapping Works

You provide a list of pairs: source column name → destination column name. Two cases exist:

| Case      | Source Column (A) | Destination Column (B) | What Meltano Does                    |
|-----------|-------------------|------------------------|--------------------------------------|
| Same name | email             | email                  | Passes through with no change        |
| Renamed   | name              | customer_name          | Renames via column_renames in config  |
| Renamed   | created_at        | signup_date            | Renames via column_renames in config  |

Columns in the source table that are NOT in your mapping are never read or written — they are completely skipped.

## 4. meltano.yml Example

Complete config for migrating PostgreSQL table `users` to `customers` with three columns, two of which are renamed:

```yaml
# meltano.yml
version: 1

plugins:
  extractors:
    - name: tap-postgres
      variant: meltanolabs
      select:
        - "users.name"
        - "users.email"
        - "users.created_at"
      metadata:
        "users":
          replication-method: FULL_TABLE

  loaders:
    - name: target-postgres
      variant: meltanolabs
      config:
        default_target_schema: public
        table_name: customers
        column_renames:
          name: customer_name        # A.name  -> B.customer_name
          created_at: signup_date    # A.created_at -> B.signup_date
        # email has same name — not listed in column_renames

jobs:
  - name: users-to-customers
    tasks:
      - tap-postgres target-postgres
```

## 5. Singer catalog.json — Column Selection

The catalog tells the tap which columns to extract. Only columns listed with `selected: true` are included in the data stream:

```json
{
  "streams": [{
    "tap_stream_id": "users",
    "stream": "users",
    "schema": {
      "type": "object",
      "properties": {
        "name":       { "type": ["null", "string"] },
        "email":      { "type": ["null", "string"] },
        "created_at": { "type": ["null", "string"] }
      }
    },
    "metadata": [
      { "breadcrumb": [], "metadata": { "selected": true } },
      { "breadcrumb": ["properties","name"],       "metadata": { "selected": true } },
      { "breadcrumb": ["properties","email"],      "metadata": { "selected": true } },
      { "breadcrumb": ["properties","created_at"], "metadata": { "selected": true } }
    ]
  }]
}
```

## 6. Running the Migration

### Step 1 — Install plugins

```bash
meltano install
```

### Step 2 — Verify source columns

```bash
meltano invoke tap-postgres --discover
```

### Step 3 — Run migration

Always pass `--job-id` with a unique value per run. This prevents state corruption when multiple pipelines run at the same time:

```bash
meltano run --job-id users-to-customers-$(date +%s) tap-postgres target-postgres
```

### Step 4 — Check state

```bash
meltano state get users-to-customers-$(date +%s)
```

## 7. Sync Modes

| Mode         | What It Does                                              | Use When                                      |
|--------------|------------------------------------------------------------|-----------------------------------------------|
| Full Table   | Copies all rows from A to B every run                      | Small tables, one-time migration              |
| Incremental  | Only copies rows newer than last run (needs replication key) | Large tables with updated_at column         |
| Upsert       | Inserts new rows, updates rows that already exist in B     | Ongoing sync without duplicates               |

## 8. Pre-Migration Checklist

- [ ] Source table exists and credentials are correct
- [ ] Destination table already exists with the B column names created
- [ ] Every column in your mapping actually exists in the source table
- [ ] `column_renames` in meltano.yml only lists columns that are renamed (not same-name ones)
- [ ] `meltano install` has been run after any meltano.yml changes
- [ ] `--job-id` is unique per run (use timestamp suffix)
- [ ] For incremental mode: source table has an `updated_at` or similar column

## 9. Common Errors & Fixes

| Error              | Cause                                      | Fix                                                    |
|--------------------|--------------------------------------------|--------------------------------------------------------|
| Column not found   | Column in mapping does not exist in source | Run `--discover` and check exact names                 |
| Table does not exist | Destination table not created yet        | `CREATE TABLE` in destination first                    |
| Duplicate key      | Running Full Table into table with unique index | Use Upsert mode or TRUNCATE first                  |
| No records selected | `selected: false` in catalog metadata     | Set `selected: true` for each column                  |
| Rename not applied | `column_renames` key misspelled           | Check indentation and exact source name               |

## 10. Integration with MANTrixFlow API

The ETL service `POST /run-meltano-pipeline` supports dynamic connections with full table-to-table mapping:

| Feature | API Support |
|---------|-------------|
| `source_table` | ✓ Stream filter (extractor) |
| `source_schema` | ✓ Stream name for stream_maps key |
| `dest_table` | ✓ Stream alias via stream_maps |
| `column_renames` | ✓ `{ source_col: dest_col }` via stream_maps |

**Payload example:**
```json
{
  "direction": "postgres-to-postgres",
  "source_connection_config": {...},
  "dest_connection_config": {...},
  "source_table": "users",
  "source_schema": "public",
  "dest_table": "customers",
  "column_renames": {
    "name": "customer_name",
    "created_at": "signup_date"
  }
}
```

Pipeline transformations with `transformType: "rename"` are automatically converted to `column_renames` when running via the API or ETL jobs queue.
