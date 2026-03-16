"""Build dlt source objects for FULL_TABLE, INCREMENTAL, and LOG_BASED sync."""

from __future__ import annotations

from typing import Any

import dlt
from dlt.sources.sql_database import sql_database, sql_table

from pg_replication import init_replication, replication_resource
from dlt.sources.credentials import ConnectionStringCredentials


def build_full_table_source(
    conn_str: str,
    table_names: list[str],
    schema: str,
    backend: str,
    chunk_size: int,
):
    """
    Returns dlt source for FULL_TABLE sync.
    Uses sql_database with table_names for performance on large schemas.
    """
    return sql_database(
        credentials=conn_str,
        schema=schema,
        table_names=table_names,
        backend=backend,
        chunk_size=chunk_size,
    )


def build_incremental_source(
    conn_str: str,
    table_name: str,
    replication_key: str,
    initial_value: Any,
    schema: str,
    backend: str,
    chunk_size: int,
):
    """
    Returns dlt resource for INCREMENTAL sync using dlt.sources.incremental.
    """
    incremental = dlt.sources.incremental(
        replication_key,
        initial_value=initial_value,
    )
    return sql_table(
        credentials=conn_str,
        table=table_name,
        schema=schema,
        incremental=incremental,
        backend=backend,
        chunk_size=chunk_size,
    )


def build_cdc_snapshot_source(
    pg_conn_str: str,
    slot_name: str,
    pub_name: str,
    schema_name: str,
    table_names: list[str],
):
    """
    Returns snapshot resources from init_replication for the initial CDC full load.
    Only called on first CDC run (when last_state is None).
    """
    creds = ConnectionStringCredentials(pg_conn_str)
    return init_replication(
        slot_name=slot_name,
        pub_name=pub_name,
        schema_name=schema_name,
        table_names=table_names,
        credentials=creds,
        persist_snapshots=True,
    )


def build_cdc_changes_source(
    pg_conn_str: str,
    slot_name: str,
    pub_name: str,
    initial_last_commit_lsn: int | None = None,
):
    """
    Returns replication_resource for ongoing CDC changes.
    Only called on subsequent CDC runs.
    """
    creds = ConnectionStringCredentials(pg_conn_str)
    return replication_resource(
        slot_name=slot_name,
        pub_name=pub_name,
        credentials=creds,
        initial_last_commit_lsn=initial_last_commit_lsn,
    )
