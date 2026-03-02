# MANTrixFlow ETL — Coding Conventions

## Scope

- **Sources:** PostgreSQL, MongoDB only
- **Destination:** PostgreSQL only
- **Tool:** dlt (Data Load Tool) — [dlthub.com](https://dlthub.com)

## Folder Structure

```
apps/new-etl/
├── api/                    # FastAPI routes
│   ├── main.py
│   └── routes/
├── config/                 # Shared config — NO duplication
│   └── connections.py     # build_postgres_conn_str, build_mongo_conn_url
├── core/                   # dlt orchestration
│   └── dlt_runner.py       # Thin wrapper over dlt verified sources
├── pg_replication/         # dlt verified source (Postgres WAL CDC)
├── mongodb/                # dlt verified source
├── requirements.txt
├── Dockerfile
└── CONVENTIONS.md
```

## Rules

### 1. dlt-only — No Custom Implementations

- Use **dlt verified sources** only: `pg_replication`, `mongodb`, `sql_database`
- Do NOT add custom PyMongo `find()`, custom SQL queries for sync, Debezium, or other CDC libs
- Postgres full: `dlt.sources.sql_database.sql_table`
- Postgres incremental: `pg_replication` (init_replication, replication_resource)
- MongoDB full: `mongodb_collection`
- MongoDB incremental: `mongodb_collection(..., incremental=dlt.sources.incremental(cursor_field))`

### 2. Credentials — No Hardcoding

- **Credentials are never read from `.dlt/secrets.toml` for API runs.**
- They come from the request payload: NestJS fetches from `data_source_connections` (DB), decrypts, and passes `source_config` / `dest_config` in `POST /sync/run-sync`.
- `.dlt/secrets.toml` is a dlt init template; for MANTrixFlow, all syncs are API-driven.

### 3. Shared Config — No Duplication

- All connection strings: `config.connections.build_postgres_conn_str`, `build_mongo_conn_url`
- Stream parsing: `config.connections.parse_stream`
- Do NOT duplicate connection logic in routes or core

### 4. Minimal Dependencies

- `dlt[postgres]` — dlt + Postgres destination
- `pymongo` — MongoDB source (from dlt mongodb)
- `fastapi`, `uvicorn` — API server
- No extra third-party: no Debezium, no custom CDC libs

### 5. Sync Modes

| Source   | Full       | Incremental                    |
|----------|------------|--------------------------------|
| Postgres | sql_table  | pg_replication (WAL, log-based)|
| MongoDB  | mongodb_collection | mongodb_collection + dlt.sources.incremental(cursor_field) |

- Postgres incremental: **log-based only** — no cursor/column
- MongoDB incremental: **cursor-based** — requires `cursor_field` (e.g. `_id`, `updatedAt`)

### 6. References

- [dlt Intro](https://dlthub.com/docs/intro)
- [Postgres replication](https://dlthub.com/docs/dlt-ecosystem/verified-sources/pg_replication)
- [MongoDB](https://dlthub.com/docs/dlt-ecosystem/verified-sources/mongodb)
- [Postgres destination](https://dlthub.com/docs/dlt-ecosystem/destinations/postgres)
