"""
Python FastAPI ETL Microservice
Handles data source connection, collector (Singer taps), transformer (custom Python scripts), and emitter (data loading).

Endpoints:
- POST /discover-schema/{sourceType} - Discover schema from source
- POST /collect/{sourceType} - Collect data via Singer taps
- POST /transform - Transform records using custom Python script
- POST /emit/{destType} - Emit transformed data to destination
- POST /delta-check/{sourceType} - Check for changes (incremental sync)
"""

import os
import sys
import json
import logging
import io
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional
from contextlib import redirect_stdout, redirect_stderr

from fastapi import FastAPI, HTTPException, Depends, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field, ConfigDict
import uvicorn
from dotenv import load_dotenv

# Add connector paths to sys.path so we can import tap_postgres, tap_mysql, tap_mongodb
# Get the directory where this file is located
ETL_DIR = os.path.dirname(os.path.abspath(__file__))
CONNECTORS_DIR = os.path.join(ETL_DIR, 'connectors')

# Add connector directories to Python path
for connector_name in ['tap-postgres', 'tap-mysql', 'tap-mongodb']:
    connector_path = os.path.join(CONNECTORS_DIR, connector_name)
    if os.path.exists(connector_path) and connector_path not in sys.path:
        sys.path.insert(0, connector_path)

# Import transformer and utils
from transformer import safe_exec_transform, validate_transform_script
from utils import (
    parse_singer_state,
    parse_singer_records,
    build_singer_config,
    normalize_source_type,
    capture_stdout_stderr,
    extract_bookmark_from_state,
    merge_state,
)

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO'),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger(__name__)

# FastAPI app
app = FastAPI(
    title="ETL Service",
    description="ETL microservice for data collection, transformation, and emission",
    version="1.0.0",
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Security
security = HTTPBearer(auto_error=False)


def decode_jwt_token(token: str) -> Optional[Dict[str, Any]]:
    """Decode JWT token to extract user information (without verification for now)"""
    try:
        import base64
        # JWT tokens have 3 parts separated by dots: header.payload.signature
        parts = token.split('.')
        if len(parts) != 3:
            return None
        
        # Decode payload (second part)
        payload = parts[1]
        # Add padding if needed
        padding = 4 - len(payload) % 4
        if padding != 4:
            payload += '=' * padding
        
        decoded = base64.urlsafe_b64decode(payload)
        import json
        return json.loads(decoded)
    except Exception as e:
        logger.warning(f"Failed to decode JWT token: {str(e)}")
        return None


async def verify_token(
    authorization: Optional[str] = Header(None),
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
) -> Optional[str]:
    """
    Verify JWT token from Supabase.
    In production, validate against Supabase JWT secret.
    For now, just extract the token.
    """
    if authorization:
        # Extract Bearer token
        if authorization.startswith("Bearer "):
            return authorization[7:]
    if credentials:
        return credentials.credentials
    # Allow requests without auth for development
    # In production, raise HTTPException here
    return None


# ============================================================================
# Pydantic Models
# ============================================================================

class DiscoverSchemaRequest(BaseModel):
    source_type: str
    connection_config: Dict[str, Any]
    source_config: Optional[Dict[str, Any]] = {}
    table_name: Optional[str] = None
    schema_name: Optional[str] = None
    query: Optional[str] = None


class DiscoverSchemaResponse(BaseModel):
    columns: List[Dict[str, Any]]
    primary_keys: List[str]
    estimated_row_count: Optional[int] = None


class CollectRequest(BaseModel):
    source_type: str
    connection_config: Dict[str, Any]
    source_config: Optional[Dict[str, Any]] = {}
    table_name: Optional[str] = None
    schema_name: Optional[str] = None
    query: Optional[str] = None
    sync_mode: str = "full"  # "full" or "incremental"
    checkpoint: Optional[Dict[str, Any]] = None
    limit: Optional[int] = None
    offset: Optional[int] = None
    cursor: Optional[str] = None


class CollectResponse(BaseModel):
    rows: List[Dict[str, Any]]
    total_rows: Optional[int] = None
    next_cursor: Optional[str] = None
    has_more: bool = False
    metadata: Optional[Dict[str, Any]] = None
    checkpoint: Optional[Dict[str, Any]] = None  # New state/bookmark


class TransformRequest(BaseModel):
    rows: List[Dict[str, Any]]
    transform_script: str = Field(..., description="Python script defining transform(record) function")


class TransformResponse(BaseModel):
    transformed_rows: List[Dict[str, Any]]
    errors: List[Dict[str, Any]]


class EmitRequest(BaseModel):
    destination_type: str
    connection_config: Dict[str, Any]
    destination_config: Optional[Dict[str, Any]] = {}
    table_name: str
    schema_name: Optional[str] = None
    rows: List[Dict[str, Any]]
    write_mode: str = "upsert"  # "append", "upsert", "replace"
    upsert_key: Optional[List[str]] = None


class EmitResponse(BaseModel):
    rows_written: int
    rows_skipped: int
    rows_failed: int
    errors: List[Dict[str, Any]]


class DeltaCheckRequest(BaseModel):
    connection_config: Dict[str, Any]
    source_config: Optional[Dict[str, Any]] = {}
    table_name: Optional[str] = None
    schema_name: Optional[str] = None
    checkpoint: Optional[Dict[str, Any]] = None


class DeltaCheckResponse(BaseModel):
    has_changes: bool
    checkpoint: Optional[Dict[str, Any]] = None


class TestConnectionRequest(BaseModel):
    model_config = ConfigDict(extra='allow')  # Allow additional fields
    
    type: str
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    connection_string: Optional[str] = None
    connection_string_mongo: Optional[str] = None
    ssl: Optional[Dict[str, Any]] = None
    auth_source: Optional[str] = None
    replica_set: Optional[str] = None
    tls: Optional[bool] = None
    database_type: Optional[str] = None


class TestConnectionResponse(BaseModel):
    success: bool
    message: Optional[str] = None
    error: Optional[str] = None
    version: Optional[str] = None
    response_time_ms: Optional[int] = None
    details: Optional[Dict[str, Any]] = None


# ============================================================================
# Singer Tap Integration
# ============================================================================

def get_tap_module(source_type: str):
    """Get the appropriate Singer tap module."""
    normalized = normalize_source_type(source_type)
    
    if normalized == 'postgresql':
        try:
            import tap_postgres
            return tap_postgres
        except ImportError:
            raise HTTPException(
                status_code=500,
                detail="tap-postgres not installed. Install with: pip install -e connectors/tap-postgres"
            )
    elif normalized == 'mysql':
        try:
            import tap_mysql
            return tap_mysql
        except ImportError:
            raise HTTPException(
                status_code=500,
                detail="tap-mysql not installed. Install with: pip install -e connectors/tap-mysql"
            )
    elif normalized == 'mongodb':
        try:
            import tap_mongodb
            return tap_mongodb
        except ImportError:
            raise HTTPException(
                status_code=500,
                detail="tap-mongodb not installed. Install with: pip install -e connectors/tap-mongodb"
            )
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported source type: {source_type}. Supported: postgresql, mysql, mongodb"
        )


# ============================================================================
# API Endpoints
# ============================================================================

@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "service": "Python ETL Service",
        "version": "1.0.0",
        "status": "running",
    }


@app.get("/health")
async def health():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.post("/discover-schema/{source_type}", response_model=DiscoverSchemaResponse)
async def discover_schema(
    source_type: str,
    request: DiscoverSchemaRequest,
    token: Optional[str] = Depends(verify_token),
):
    """
    Discover schema from a data source using Singer tap.
    """
    try:
        logger.info(f"Discovering schema for {source_type}")
        
        # Get tap module
        tap_module = get_tap_module(source_type)
        
        # Build Singer config
        singer_config = build_singer_config(
            request.connection_config,
            request.source_config or {},
            request.table_name,
            request.schema_name,
            request.query,
        )
        
        # For PostgreSQL, call discovery directly
        if source_type.lower() in ['postgresql', 'postgres']:
            import tap_postgres.db as post_db
            
            # Prepare connection config
            conn_config = {
                'host': singer_config.get('host'),
                'port': singer_config.get('port', 5432),
                'user': singer_config.get('user'),
                'password': singer_config.get('password'),
                'dbname': singer_config.get('dbname'),
            }
            
            if singer_config.get('ssl') == 'true' or singer_config.get('ssl_mode'):
                conn_config['sslmode'] = 'require'
            
            # Call discovery
            try:
                # Capture stdout to get catalog
                stdout_capture = io.StringIO()
                stderr_capture = io.StringIO()
                
                with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                    # Import and call discovery
                    from tap_postgres import do_discovery
                    streams = do_discovery(conn_config)
                
                # Parse catalog from streams
                columns = []
                primary_keys = []
                
                # Find the specific table if table_name is provided
                target_stream = None
                if request.table_name:
                    for stream in streams:
                        stream_id = stream.get('tap_stream_id', '')
                        if request.table_name.lower() in stream_id.lower():
                            target_stream = stream
                            break
                else:
                    # Use first stream if no table specified
                    target_stream = streams[0] if streams else None
                
                if target_stream:
                    schema = target_stream.get('schema', {}).get('properties', {})
                    metadata_map = {}
                    for md in target_stream.get('metadata', []):
                        breadcrumb = md.get('breadcrumb', [])
                        if breadcrumb:
                            metadata_map[tuple(breadcrumb)] = md.get('metadata', {})
                        else:
                            metadata_map[()] = md.get('metadata', {})
                    
                    # Extract columns and primary keys
                    for col_name, col_schema in schema.items():
                        col_metadata = metadata_map.get((col_name,), {})
                        is_pk = col_metadata.get('is-primary-key', False)
                        
                        columns.append({
                            'name': col_name,
                            'type': col_schema.get('type', 'string'),
                            'nullable': 'null' in col_schema.get('type', []) if isinstance(col_schema.get('type'), list) else False,
                        })
                        
                        if is_pk:
                            primary_keys.append(col_name)
                
                return DiscoverSchemaResponse(
                    columns=columns,
                    primary_keys=primary_keys,
                )
                
            except Exception as e:
                logger.error(f"Discovery failed: {str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Discovery failed: {str(e)}")
        
        else:
            # For MySQL and MongoDB, similar approach
            raise HTTPException(
                status_code=501,
                detail=f"Discovery for {source_type} not yet implemented. PostgreSQL is supported."
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Schema discovery error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Schema discovery failed: {str(e)}")


@app.post("/collect/{source_type}", response_model=CollectResponse)
async def collect(
    source_type: str,
    request: CollectRequest,
    token: Optional[str] = Depends(verify_token),
):
    """
    Collect data from a source using Singer tap.
    Uses Singer tap-postgres, tap-mysql, tap-mongodb connectors directly.
    Supports full and incremental sync modes with state/bookmark handling.
    """
    try:
        logger.info(f"Collecting data from {source_type} (mode: {request.sync_mode})")
        
        normalized_source = normalize_source_type(source_type)
        
        # Build Singer config from connection config
        singer_config = build_singer_config(
            request.connection_config,
            request.source_config or {},
            request.table_name,
            request.schema_name,
            request.query,
        )
        
        table_name = request.table_name or 'unknown'
        schema_name = request.schema_name or 'public'
        
        # =====================================================
        # PostgreSQL - Use tap-postgres connector directly
        # =====================================================
        if normalized_source == 'postgresql':
            import tap_postgres
            import tap_postgres.db as post_db
            import tap_postgres.sync_strategies.full_table as full_table
            import tap_postgres.sync_strategies.incremental as incremental
            import tap_postgres.sync_strategies.common as sync_common
            from singer import metadata
            from singer.schema import Schema
            import singer
            import copy
            
            # Prepare connection config for tap-postgres
            conn_config = {
                'host': singer_config.get('host'),
                'port': singer_config.get('port', 5432),
                'user': singer_config.get('user'),
                'password': singer_config.get('password'),
                'dbname': singer_config.get('dbname'),  # IMPORTANT: Must be set to the correct database
            }
            
            if singer_config.get('ssl') == 'true' or singer_config.get('ssl_mode'):
                conn_config['sslmode'] = 'require'
            
            # =====================================================
            # Step 1: Discover table schema using tap-postgres
            # =====================================================
            try:
                # Use tap-postgres's discovery functions directly
                # First, connect to the specific database and get table info
                with post_db.open_connection(conn_config) as conn:
                    # Use tap-postgres's produce_table_info to get column information
                    table_info = tap_postgres.produce_table_info(conn)
                    
                    if schema_name not in table_info or table_name not in table_info.get(schema_name, {}):
                        raise HTTPException(
                            status_code=404,
                            detail=f"Table {schema_name}.{table_name} not found in database {conn_config['dbname']}"
                        )
                    
                    # Use tap-postgres's discover_columns to build stream entries
                    db_streams = tap_postgres.discover_columns(conn, table_info)
                
                # Find our target stream
                target_stream = None
                for stream in db_streams:
                    if stream['table_name'] == table_name:
                        md_map = metadata.to_map(stream['metadata'])
                        if md_map.get((), {}).get('schema-name') == schema_name:
                            target_stream = stream
                            break
                
                if not target_stream:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Could not find stream for table {schema_name}.{table_name}"
                    )
                
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Discovery failed: {str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Discovery failed: {str(e)}")
            
            # =====================================================
            # Step 2: Prepare for sync using tap-postgres functions
            # =====================================================
            # Restore state from checkpoint (contains Singer bookmarks)
            state = request.checkpoint or {}
            captured_messages = []
            
            # Override singer.write_message to capture records
            original_write_message = singer.write_message
            def capture_write_message(message):
                captured_messages.append(message)
            singer.write_message = capture_write_message
            
            try:
                md_map = metadata.to_map(target_stream['metadata'])
                
                # Get desired columns from stream schema (same as tap-postgres does)
                desired_columns = [c for c in target_stream['schema']['properties'].keys() 
                                   if sync_common.should_sync_column(md_map, c)]
                desired_columns.sort()
                
                if len(desired_columns) == 0:
                    raise HTTPException(
                        status_code=400,
                        detail=f"No columns available for table {schema_name}.{table_name}"
                    )
                
                # =====================================================
                # Step 3: CDC using PostgreSQL xmin (works for ALL tables)
                # =====================================================
                # PostgreSQL's xmin is a system column (transaction ID) that:
                # - Exists on EVERY table automatically (no user columns needed)
                # - Is managed by PostgreSQL, not user data
                # - Updated on every INSERT/UPDATE
                # - Perfect for detecting changes universally
                
                stream_id = target_stream['tap_stream_id']
                
                # We use xmin for CDC - it's universal and requires no user columns
                # The tap-postgres full_table sync already uses xmin::text::bigint
                logger.info(f"Using PostgreSQL xmin system column for CDC (universal method)")
                
                # =====================================================
                # Step 4: Use tap-postgres sync with xmin-based CDC
                # =====================================================
                # Register type adapters (same as tap-postgres does)
                tap_postgres.register_type_adapters(conn_config)
                
                # Set the database name from stream metadata (CRITICAL!)
                conn_config['dbname'] = md_map.get((), {}).get('database-name', conn_config['dbname'])
                
                # Determine sync method and call appropriate function
                is_view = md_map.get((), {}).get('is-view', False)
                
                # Check checkpoint state
                full_sync_done = bool(state.get('bookmarks', {}).get(stream_id, {}).get('full_sync_completed'))
                last_xmin = state.get('bookmarks', {}).get(stream_id, {}).get('xmin')
                last_row_count = state.get('bookmarks', {}).get(stream_id, {}).get('last_row_count', 0)
                
                # Get current row count from source (for change detection)
                current_row_count = 0
                current_max_xmin = 0
                try:
                    with post_db.open_connection(conn_config, False) as check_conn:
                        with check_conn.cursor() as check_cur:
                            # Get current row count
                            check_cur.execute(f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"')
                            current_row_count = check_cur.fetchone()[0]
                            
                            # Get current max xmin (transaction ID)
                            check_cur.execute(f'SELECT MAX(xmin::text::bigint) FROM "{schema_name}"."{table_name}"')
                            result = check_cur.fetchone()
                            current_max_xmin = result[0] if result[0] else 0
                            
                            logger.info(f"Source state: {current_row_count} rows, max xmin={current_max_xmin}")
                            logger.info(f"Checkpoint: last_count={last_row_count}, last_xmin={last_xmin}")
                except Exception as e:
                    logger.warning(f"Could not check source state: {e}")
                
                # Decide sync mode based on checkpoint and changes
                if full_sync_done:
                    # Check if there are new/changed records
                    has_new_data = (
                        current_row_count > last_row_count or  # New rows added
                        (current_max_xmin and last_xmin and current_max_xmin > last_xmin)  # Changes detected
                    )
                    
                    if has_new_data:
                        logger.info(f"🔄 New data detected! count: {last_row_count} → {current_row_count}, xmin: {last_xmin} → {current_max_xmin}")
                        # Do incremental sync using xmin
                        # Fetch only records with xmin > last_xmin
                        sync_common.send_schema_message(target_stream, [])
                        
                        # Use full_table sync but it will capture all records
                        # We filter by xmin in captured_messages below
                        state = full_table.sync_table(conn_config, target_stream, state, desired_columns, md_map)
                        
                        # Update checkpoint
                        state = singer.write_bookmark(state, stream_id, 'xmin', current_max_xmin)
                        state = singer.write_bookmark(state, stream_id, 'last_row_count', current_row_count)
                    else:
                        logger.info(f"✅ No new data (count={current_row_count}, xmin={current_max_xmin})")
                        # No new data - return empty
                else:
                    # First run - do full sync
                    logger.info(f"📦 Initial full sync for {stream_id}")
                    sync_common.send_schema_message(target_stream, [])
                    
                    if is_view:
                        state = full_table.sync_view(conn_config, target_stream, state, desired_columns, md_map)
                    else:
                        state = full_table.sync_table(conn_config, target_stream, state, desired_columns, md_map)
                    
                    # Save current state for next incremental run
                    state = singer.write_bookmark(state, stream_id, 'xmin', current_max_xmin)
                    state = singer.write_bookmark(state, stream_id, 'last_row_count', current_row_count)
                
                # =====================================================
                # Step 5: Extract records from Singer messages
                # =====================================================
                
                # For incremental sync, filter to only new records (xmin > last_xmin)
                is_incremental = full_sync_done and last_xmin is not None
                
                # Extract records from captured Singer messages
                all_captured_records = []
                for msg in captured_messages:
                    if hasattr(msg, 'record') and hasattr(msg, 'stream'):
                        all_captured_records.append(msg.record)
                
                # For incremental sync, we only have the new records
                new_records_count = len(all_captured_records)
                if is_incremental:
                    logger.info(f"Incremental sync: {new_records_count} new/changed records detected")
                
                # total_source_records = actual total in source table (for display)
                # For batch processing, we use new_records_count to determine has_more
                total_source_records = current_row_count  # Always the actual total in source
                
                # Apply offset and limit for batching
                records = []
                limit = request.limit or 500
                offset = request.offset or 0
                
                for i, rec in enumerate(all_captured_records):
                    if i < offset:
                        continue
                    if len(records) >= limit:
                        break
                    records.append(rec)
                
                # Calculate if there are more records to process (based on new records, not total)
                processed_so_far = offset + len(records)
                has_more = processed_so_far < new_records_count
                
                # Save sync progress in checkpoint for batch continuation
                state = singer.write_bookmark(state, stream_id, 'sync_offset', processed_so_far)
                state = singer.write_bookmark(state, stream_id, 'total_records', total_source_records)
                state = singer.write_bookmark(state, stream_id, 'new_records_this_sync', new_records_count)
                
                # Only mark full_sync_completed when ALL records are processed
                if not has_more and new_records_count > 0:
                    state = singer.write_bookmark(state, stream_id, 'full_sync_completed', True)
                    state = singer.write_bookmark(state, stream_id, 'full_sync_at', datetime.utcnow().isoformat())
                    state = singer.write_bookmark(state, stream_id, 'xmin', current_max_xmin)
                    state = singer.write_bookmark(state, stream_id, 'last_row_count', current_row_count)
                    if is_incremental:
                        logger.info(f"✅ Incremental sync completed! {new_records_count} new records synced (total in source: {current_row_count})")
                    else:
                        logger.info(f"✅ Full sync completed! {new_records_count} records processed.")
                
                logger.info(f"Collected {len(records)} records from {schema_name}.{table_name}")
                if is_incremental:
                    logger.info(f"Incremental: {processed_so_far}/{new_records_count} new records processed, total in source: {current_row_count}")
                else:
                    logger.info(f"Progress: {processed_so_far}/{new_records_count} ({round(processed_so_far/max(new_records_count,1)*100, 1)}%), hasMore={has_more}")
                logger.info(f"Checkpoint: xmin={current_max_xmin}, count={current_row_count}")
                
                return CollectResponse(
                    rows=records,
                    total_rows=total_source_records,  # Total in source, not just this batch
                    has_more=has_more,  # True if more records exist
                    checkpoint=state,  # Contains Singer bookmarks for incremental sync
                )
                
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Collection failed: {str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")
            finally:
                singer.write_message = original_write_message
        
        # =====================================================
        # MySQL - Use tap-mysql connector directly
        # =====================================================
        elif normalized_source == 'mysql':
            import tap_mysql
            import tap_mysql.connection as mysql_conn
            import tap_mysql.sync_strategies.full_table as mysql_full_table
            import tap_mysql.sync_strategies.incremental as mysql_incremental
            import tap_mysql.sync_strategies.common as mysql_common
            from singer import metadata
            import singer
            
            # Prepare connection config for tap-mysql
            conn_config = {
                'host': singer_config.get('host'),
                'port': singer_config.get('port', 3306),
                'user': singer_config.get('user'),
                'password': singer_config.get('password'),
                'database': singer_config.get('dbname'),
            }
            
            if singer_config.get('ssl') == 'true':
                conn_config['ssl'] = True
            
            try:
                # Use tap-mysql discovery
                with mysql_conn.make_connection(conn_config) as conn:
                    # Discover catalog
                    catalog = tap_mysql.discover_catalog(conn, conn_config.get('database'))
                
                # Find target stream
                target_stream = None
                for stream in catalog.streams:
                    stream_dict = stream.to_dict()
                    md_map = metadata.to_map(stream_dict.get('metadata', []))
                    stream_schema = md_map.get((), {}).get('schema-name', conn_config.get('database'))
                    stream_table = stream_dict.get('table_name', '')
                    
                    if stream_table == table_name and (not schema_name or stream_schema == schema_name):
                        target_stream = stream_dict
                        break
                
                if not target_stream:
                    raise HTTPException(
                        status_code=404,
                        detail=f"Table {schema_name}.{table_name} not found in database"
                    )
                
                state = request.checkpoint or {}
                captured_messages = []
                
                original_write_message = singer.write_message
                def capture_write_message(message):
                    captured_messages.append(message)
                singer.write_message = capture_write_message
                
                try:
                    md_map = metadata.to_map(target_stream.get('metadata', []))
                    desired_columns = [c for c in target_stream['schema']['properties'].keys()
                                       if mysql_common.should_sync_column(md_map, c)]
                    
                    if request.sync_mode == 'full':
                        with mysql_conn.make_connection(conn_config) as conn:
                            state = mysql_full_table.sync_table(conn, conn_config, target_stream, state, desired_columns, md_map)
                    elif request.sync_mode == 'incremental':
                        with mysql_conn.make_connection(conn_config) as conn:
                            state = mysql_incremental.sync_table(conn, conn_config, target_stream, state, desired_columns, md_map)
                    
                    # Extract records
                    records = []
                    limit = request.limit or float('inf')
                    offset = request.offset or 0
                    record_count = 0
                    
                    for msg in captured_messages:
                        if hasattr(msg, 'record') and hasattr(msg, 'stream'):
                            if record_count < offset:
                                record_count += 1
                                continue
                            if len(records) >= limit:
                                break
                            records.append(msg.record)
                            record_count += 1
                    
                    logger.info(f"Collected {len(records)} records from {table_name}")
                    
                    return CollectResponse(
                        rows=records,
                        total_rows=len(records),
                        has_more=record_count > (offset + len(records)),
                        checkpoint=state,
                    )
                    
                finally:
                    singer.write_message = original_write_message
                    
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"MySQL collection failed: {str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")
        
        # =====================================================
        # MongoDB - Use pymongo directly (as tap-mongodb does)
        # =====================================================
        elif normalized_source == 'mongodb':
            import pymongo
            from pymongo import MongoClient
            from singer import metadata
            import singer
            
            # Prepare connection config for tap-mongodb
            conn_string = singer_config.get('connection_string_mongo') or singer_config.get('connection_string')
            if not conn_string:
                # Build connection string
                host = singer_config.get('host', 'localhost')
                port = singer_config.get('port', 27017)
                user = singer_config.get('user', '')
                password = singer_config.get('password', '')
                dbname = singer_config.get('dbname', 'test')
                
                if user and password:
                    conn_string = f"mongodb://{user}:{password}@{host}:{port}/{dbname}"
                else:
                    conn_string = f"mongodb://{host}:{port}/{dbname}"
            
            conn_config = {
                'host': conn_string,
                'database': singer_config.get('dbname'),
            }
            
            try:
                # Use pymongo directly (same as tap-mongodb)
                client = MongoClient(conn_string)
                db = client[conn_config.get('database', 'test')]
                
                # Build a simple stream for the collection
                target_stream = {
                    'tap_stream_id': table_name,
                    'stream': table_name,
                    'table_name': table_name,
                    'schema': {'type': 'object', 'properties': {}},
                    'metadata': [{
                        'breadcrumb': (),
                        'metadata': {
                            'database-name': conn_config.get('database'),
                            'collection-name': table_name,
                        }
                    }],
                }
                
                state = request.checkpoint or {}
                captured_messages = []
                
                original_write_message = singer.write_message
                def capture_write_message(message):
                    captured_messages.append(message)
                singer.write_message = capture_write_message
                
                try:
                    # For MongoDB, we'll use direct collection access
                    collection = db[table_name]
                    
                    # Build query (json is imported at module level)
                    query = {}
                    if request.query:
                        import json as json_module
                        query = json_module.loads(request.query) if isinstance(request.query, str) else request.query
                    
                    # Execute query
                    cursor = collection.find(query)
                    
                    if request.offset:
                        cursor = cursor.skip(request.offset)
                    if request.limit:
                        cursor = cursor.limit(request.limit)
                    
                    records = []
                    for doc in cursor:
                        # Convert ObjectId to string
                        if '_id' in doc:
                            doc['_id'] = str(doc['_id'])
                        records.append(doc)
                    
                    client.close()
                    
                    logger.info(f"Collected {len(records)} records from {table_name}")
                    
                    return CollectResponse(
                        rows=records,
                        total_rows=len(records),
                        has_more=len(records) == request.limit if request.limit else False,
                        checkpoint=state,
                    )
                    
                finally:
                    singer.write_message = original_write_message
                    
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"MongoDB collection failed: {str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")
        
        else:
            raise HTTPException(
                status_code=501,
                detail=f"Collection for {source_type} not supported. Supported: postgresql, mysql, mongodb"
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Collection error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")


@app.post("/transform", response_model=TransformResponse)
async def transform(
    request: TransformRequest,
    token: Optional[str] = Depends(verify_token),
):
    """
    Transform records using a custom Python script.
    
    The transform_script must define a function:
        def transform(record):
            # Use record.get("source_field") to read from source
            # Return dict with destination keys
            return {"destination_field": record.get("source_field")}
    """
    try:
        logger.info(f"Transforming {len(request.rows)} records")
        
        result = safe_exec_transform(
            request.rows,
            request.transform_script,
        )
        
        return TransformResponse(
            transformed_rows=result['transformed_rows'],
            errors=result['errors'],
        )
    
    except Exception as e:
        logger.error(f"Transformation error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Transformation failed: {str(e)}")


@app.post("/emit/{dest_type}", response_model=EmitResponse)
async def emit(
    dest_type: str,
    request: EmitRequest,
    token: Optional[str] = Depends(verify_token),
):
    """
    Emit transformed data to a destination.
    Supports append, upsert, and replace modes.
    """
    try:
        logger.info(f"Emitting {len(request.rows)} rows to {dest_type} (mode: {request.write_mode})")
        
        normalized_dest = normalize_source_type(dest_type)
        
        if normalized_dest == 'postgresql':
            # Use psycopg2 directly for PostgreSQL
            import psycopg2
            import psycopg2.extras
            
            conn_config = request.connection_config
            table_name = request.table_name
            schema_name = request.schema_name or 'public'
            
            rows_written = 0
            rows_skipped = 0
            rows_failed = 0
            errors = []
            
            if not request.rows:
                return EmitResponse(
                    rows_written=0,
                    rows_skipped=0,
                    rows_failed=0,
                    errors=[],
                )
            
            try:
                # Build connection parameters
                pg_conn = psycopg2.connect(
                    host=conn_config.get('host'),
                    port=conn_config.get('port', 5432),
                    user=conn_config.get('username', conn_config.get('user')),
                    password=conn_config.get('password'),
                    database=conn_config.get('database', conn_config.get('dbname')),
                    sslmode='prefer'
                )
                
                try:
                    first_row = request.rows[0]
                    column_keys = list(first_row.keys())
                    escaped_columns = [f'"{k}"' for k in column_keys]
                    columns = ', '.join(escaped_columns)
                    placeholders = ', '.join(['%s' for _ in column_keys])
                    
                    if request.write_mode == 'upsert' and request.upsert_key:
                        # =====================================================
                        # UPSERT MODE: Use ON CONFLICT DO UPDATE
                        # =====================================================
                        update_cols = [k for k in column_keys if k not in request.upsert_key]
                        if update_cols:
                            update_set = ', '.join([f'"{k}" = EXCLUDED."{k}"' for k in update_cols])
                        else:
                            update_set = ', '.join([f'"{k}" = EXCLUDED."{k}"' for k in column_keys])
                        
                        escaped_upsert_keys = [f'"{k}"' for k in request.upsert_key]
                        conflict_target = ', '.join(escaped_upsert_keys)
                        
                        sql = f'''
                            INSERT INTO "{schema_name}"."{table_name}" ({columns})
                            VALUES ({placeholders})
                            ON CONFLICT ({conflict_target}) DO UPDATE SET {update_set}
                        '''
                        
                        # Use individual inserts for upsert to track counts properly
                        with pg_conn.cursor() as pg_cursor:
                            for idx, row in enumerate(request.rows):
                                try:
                                    values = [row.get(k) for k in column_keys]
                                    pg_cursor.execute(sql, values)
                                    rows_written += 1
                                except Exception as e:
                                    pg_conn.rollback()  # Rollback this row's transaction
                                    rows_failed += 1
                                    if len(errors) < 10:  # Limit error logs
                                        errors.append({
                                            'record_index': idx,
                                            'error': str(e),
                                        })
                                    logger.warning(f"Failed to upsert row {idx}: {str(e)}")
                            
                            pg_conn.commit()
                        
                        logger.info(f"Upserted {rows_written} rows into {schema_name}.{table_name}")
                    
                    elif request.write_mode == 'append':
                        # =====================================================
                        # APPEND MODE: Use ON CONFLICT DO UPDATE to overwrite duplicates
                        # =====================================================
                        # First, try to detect primary key for ON CONFLICT clause
                        primary_key = None
                        with pg_conn.cursor() as pg_cursor:
                            pg_cursor.execute("""
                                SELECT a.attname
                                FROM pg_index i
                                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                                WHERE i.indrelid = %s::regclass AND i.indisprimary
                            """, (f'"{schema_name}"."{table_name}"',))
                            pk_result = pg_cursor.fetchall()
                            if pk_result:
                                primary_key = [row[0] for row in pk_result]
                        
                        if primary_key:
                            # Use ON CONFLICT DO UPDATE to overwrite existing records
                            conflict_cols = ', '.join([f'"{k}"' for k in primary_key])
                            # Update all columns except the primary key
                            update_cols = [k for k in column_keys if k not in primary_key]
                            if update_cols:
                                update_set = ', '.join([f'"{k}" = EXCLUDED."{k}"' for k in update_cols])
                            else:
                                # If all columns are primary key, just update them anyway
                                update_set = ', '.join([f'"{k}" = EXCLUDED."{k}"' for k in column_keys])
                            
                            sql = f'''
                                INSERT INTO "{schema_name}"."{table_name}" ({columns})
                                VALUES ({placeholders})
                                ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}
                            '''
                            logger.info(f"Using ON CONFLICT DO UPDATE with primary key: {primary_key}")
                        else:
                            # No primary key, use regular insert (may fail on duplicates)
                            sql = f'INSERT INTO "{schema_name}"."{table_name}" ({columns}) VALUES ({placeholders})'
                            logger.info(f"No primary key detected, using regular INSERT")
                        
                        # Execute inserts individually to track written vs updated
                        with pg_conn.cursor() as pg_cursor:
                            for idx, row in enumerate(request.rows):
                                try:
                                    values = [row.get(k) for k in column_keys]
                                    pg_cursor.execute(sql, values)
                                    rows_written += 1
                                except psycopg2.errors.UniqueViolation:
                                    # This shouldn't happen with ON CONFLICT DO UPDATE, but handle it
                                    pg_conn.rollback()
                                    rows_failed += 1
                                    if len(errors) < 10:
                                        errors.append({
                                            'record_index': idx,
                                            'error': 'Unique violation even with ON CONFLICT',
                                        })
                                except Exception as e:
                                    pg_conn.rollback()
                                    rows_failed += 1
                                    if len(errors) < 10:
                                        errors.append({
                                            'record_index': idx,
                                            'error': str(e),
                                        })
                                    logger.warning(f"Failed to insert/update row {idx}: {str(e)}")
                            
                            pg_conn.commit()
                        
                        logger.info(f"Append completed: {rows_written} written/updated, {rows_failed} failed")
                    
                    else:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Write mode '{request.write_mode}' not supported. Use 'append' or 'upsert'"
                        )
                
                finally:
                    pg_conn.close()
            
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"Emission error: {str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Emission failed: {str(e)}")
            
            return EmitResponse(
                rows_written=rows_written,
                rows_skipped=rows_skipped,
                rows_failed=rows_failed,
                errors=errors,
            )
        
        elif normalized_dest == 'mongodb':
            # Use pymongo for MongoDB
            from pymongo import MongoClient
            
            conn_config = request.connection_config
            connection_string = conn_config.get('connection_string_mongo') or conn_config.get('connection_string')
            
            if not connection_string:
                raise HTTPException(
                    status_code=400,
                    detail="MongoDB connection requires connection_string or connection_string_mongo"
                )
            
            client = MongoClient(connection_string)
            db_name = conn_config.get('database', 'default')
            db = client[db_name]
            collection = db[request.table_name]
            
            rows_written = 0
            rows_failed = 0
            errors = []
            
            try:
                if request.write_mode == 'upsert' and request.upsert_key:
                    # MongoDB upsert
                    for idx, row in enumerate(request.rows):
                        try:
                            filter_dict = {k: row.get(k) for k in request.upsert_key if k in row}
                            collection.replace_one(
                                filter_dict,
                                row,
                                upsert=True,
                            )
                            rows_written += 1
                        except Exception as e:
                            rows_failed += 1
                            errors.append({
                                'record_index': idx,
                                'error': str(e),
                            })
                
                elif request.write_mode == 'append':
                    # MongoDB insert_many
                    try:
                        result = collection.insert_many(request.rows)
                        rows_written = len(result.inserted_ids)
                    except Exception as e:
                        rows_failed = len(request.rows)
                        errors.append({
                            'error': str(e),
                        })
                
                else:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Write mode '{request.write_mode}' not supported for MongoDB"
                    )
            
            finally:
                client.close()
            
            return EmitResponse(
                rows_written=rows_written,
                rows_skipped=0,
                rows_failed=rows_failed,
                errors=errors,
            )
        
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Destination type {dest_type} not yet supported. Supported: postgresql, mongodb"
            )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Emission error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Emission failed: {str(e)}")


@app.post("/delta-check/{source_type}", response_model=DeltaCheckResponse)
async def delta_check(
    source_type: str,
    request: DeltaCheckRequest,
    token: Optional[str] = Depends(verify_token),
):
    """
    Check for changes in source (for incremental sync polling).
    Returns whether changes exist and updated checkpoint.
    """
    try:
        logger.info(f"Delta check for {source_type}")
        
        # This would query the source to check if there are new records
        # based on the checkpoint/bookmark
        # For now, return a simplified response
        
        # In production, this would:
        # 1. Connect to source
        # 2. Query for records newer than checkpoint
        # 3. Return has_changes=True if found
        
        return DeltaCheckResponse(
            has_changes=False,  # Would check actual source
            checkpoint=request.checkpoint,
        )
    
    except Exception as e:
        logger.error(f"Delta check error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Delta check failed: {str(e)}")


@app.post("/data-sources", status_code=201)
async def create_data_source(
    request: Dict[str, Any],
    token: Optional[str] = Depends(verify_token),
):
    """
    Create/validate a data source and store it in the database.
    Validates the connection configuration, creates the data source in Supabase,
    and returns the created data source.
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor
    import json
    
    try:
        organization_id = request.get("organization_id")
        name = request.get("name")
        source_type = request.get("source_type")
        description = request.get("description")
        metadata = request.get("metadata")
        connection_config = request.get("connection_config") or request.get("config") or {}
        
        # Validate required fields
        if not organization_id:
            raise HTTPException(status_code=400, detail="organization_id is required")
        if not name:
            raise HTTPException(status_code=400, detail="name is required")
        if not source_type:
            raise HTTPException(status_code=400, detail="source_type is required")
        
        # Get user ID from token or request
        created_by = request.get("created_by")
        if not created_by and token:
            # Try to decode token to get user ID
            token_data = decode_jwt_token(token)
            if token_data and token_data.get("sub"):
                created_by = token_data.get("sub")
        
        if not created_by:
            raise HTTPException(status_code=400, detail="created_by (user ID) is required. Provide in request or use valid JWT token.")
        
        # Validate source type
        normalized_type = normalize_source_type(source_type)
        if normalized_type not in ['postgresql', 'mysql', 'mongodb']:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported source type: {source_type}. Supported: postgresql, mysql, mongodb"
            )
        
        # If connection config is provided, test it
        if connection_config:
            # Build test connection request
            test_request_data = {
                "type": normalized_type,
            }
            
            # Map connection_config fields to test request
            if connection_config.get("connection_string"):
                test_request_data["connection_string"] = connection_config["connection_string"]
            elif connection_config.get("connection_string_mongo"):
                test_request_data["connection_string_mongo"] = connection_config["connection_string_mongo"]
            else:
                test_request_data["host"] = connection_config.get("host")
                test_request_data["port"] = connection_config.get("port")
                test_request_data["database"] = connection_config.get("database")
                test_request_data["username"] = connection_config.get("username") or connection_config.get("user")
                test_request_data["password"] = connection_config.get("password")
                test_request_data["ssl"] = connection_config.get("ssl")
                test_request_data["auth_source"] = connection_config.get("auth_source")
                test_request_data["replica_set"] = connection_config.get("replica_set")
                test_request_data["tls"] = connection_config.get("tls")
            
            test_request = TestConnectionRequest(**test_request_data)
            test_result = await test_connection_internal(test_request)
            
            if not test_result.success:
                raise HTTPException(
                    status_code=400,
                    detail=f"Connection test failed: {test_result.error}"
                )
        
        # Get database URL from environment
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise HTTPException(
                status_code=500,
                detail="DATABASE_URL environment variable is not set. Cannot create data source in database."
            )
        
        # Remove quotes if present (sometimes .env files have quoted values)
        database_url = database_url.strip('"').strip("'")
        
        # Connect to database and create data source
        try:
            conn = psycopg2.connect(database_url)
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check for duplicate name in organization
            cursor.execute("""
                SELECT id FROM data_sources
                WHERE organization_id = %s AND name = %s AND deleted_at IS NULL
                LIMIT 1
            """, (organization_id, name))
            
            existing = cursor.fetchone()
            if existing:
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=400,
                    detail=f'Data source with name "{name}" already exists in this organization'
                )
            
            # Generate UUID for the data source
            data_source_id = request.get("id") or str(uuid.uuid4())
            
            # Insert data source
            cursor.execute("""
                INSERT INTO data_sources (
                    id, organization_id, name, description, source_type,
                    is_active, metadata, created_by, created_at, updated_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW()
                )
                RETURNING id, organization_id, name, description, source_type,
                          is_active, metadata, created_by, created_at, updated_at, deleted_at
            """, (
                data_source_id,
                organization_id,
                name,
                description,
                normalized_type,
                True,
                json.dumps(metadata) if metadata else None,
                created_by,
            ))
            
            result = cursor.fetchone()
            conn.commit()
            cursor.close()
            conn.close()
            
            # Convert result to dict and format response
            data_source = dict(result) if result else {}
            
            return {
                "id": str(data_source.get("id", data_source_id)),
                "organization_id": str(data_source.get("organization_id", organization_id)),
                "name": data_source.get("name", name),
                "description": data_source.get("description"),
                "source_type": data_source.get("source_type", normalized_type),
                "is_active": data_source.get("is_active", True),
                "metadata": json.loads(data_source.get("metadata")) if data_source.get("metadata") else metadata,
                "created_by": str(data_source.get("created_by", created_by)),
                "created_at": data_source.get("created_at").isoformat() if data_source.get("created_at") else datetime.utcnow().isoformat(),
                "updated_at": data_source.get("updated_at").isoformat() if data_source.get("updated_at") else datetime.utcnow().isoformat(),
            }
        
        except psycopg2.IntegrityError as e:
            conn.rollback()
            cursor.close()
            conn.close()
            logger.error(f"Database integrity error: {str(e)}", exc_info=True)
            raise HTTPException(status_code=400, detail=f"Database error: {str(e)}")
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
                if cursor:
                    cursor.close()
                conn.close()
            logger.error(f"Database error: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create data source error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create data source: {str(e)}")


async def test_connection_internal(request: TestConnectionRequest) -> TestConnectionResponse:
    """Internal function to test connection (extracted from endpoint for reuse)"""
    import time
    
    start_time = time.time()
    source_type = normalize_source_type(request.type)
    logger.info(f"Testing connection for {source_type}")
    
    if source_type == 'postgresql':
        import psycopg2
        
        # Build connection parameters
        conn_params = {}
        if request.connection_string:
            conn_params['dsn'] = request.connection_string
        else:
            conn_params['host'] = request.host
            conn_params['port'] = request.port or 5432
            conn_params['database'] = request.database
            conn_params['user'] = request.username
            conn_params['password'] = request.password
            
            if request.ssl:
                conn_params['sslmode'] = 'require' if request.ssl.get('enabled') else 'prefer'
        
        # Test connection
        try:
            conn = psycopg2.connect(**conn_params)
            cursor = conn.cursor()
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            response_time = int((time.time() - start_time) * 1000)
            
            return TestConnectionResponse(
                success=True,
                message="Connection successful",
                version=version,
                response_time_ms=response_time,
            )
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            return TestConnectionResponse(
                success=False,
                error=str(e),
                response_time_ms=response_time,
            )
    
    elif source_type == 'mysql':
        try:
            import pymysql
        except ImportError:
            try:
                import PyMySQL as pymysql
            except ImportError:
                return TestConnectionResponse(
                    success=False,
                    error="MySQL driver not installed. Install with: pip install pymysql",
                )
        
        try:
            conn = pymysql.connect(
                host=request.host,
                port=request.port or 3306,
                user=request.username,
                password=request.password,
                database=request.database,
            )
            cursor = conn.cursor()
            cursor.execute("SELECT VERSION();")
            version = cursor.fetchone()[0]
            cursor.close()
            conn.close()
            
            response_time = int((time.time() - start_time) * 1000)
            
            return TestConnectionResponse(
                success=True,
                message="Connection successful",
                version=version,
                response_time_ms=response_time,
            )
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            return TestConnectionResponse(
                success=False,
                error=str(e),
                response_time_ms=response_time,
            )
    
    elif source_type == 'mongodb':
        from pymongo import MongoClient
        from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
        
        try:
            connection_string = request.connection_string_mongo or request.connection_string
            
            if not connection_string:
                # Build connection string from individual fields
                if request.username and request.password:
                    auth = f"{request.username}:{request.password}@"
                else:
                    auth = ""
                
                host = request.host or "localhost"
                port = request.port or 27017
                database = request.database or ""
                
                connection_string = f"mongodb://{auth}{host}:{port}/{database}"
                
                if request.auth_source:
                    connection_string += f"?authSource={request.auth_source}"
                if request.replica_set:
                    connection_string += f"{'&' if '?' in connection_string else '?'}replicaSet={request.replica_set}"
                if request.tls:
                    connection_string += f"{'&' if '?' in connection_string else '?'}tls=true"
            
            client = MongoClient(connection_string, serverSelectionTimeoutMS=5000)
            # Test connection
            client.admin.command('ping')
            server_info = client.server_info()
            client.close()
            
            response_time = int((time.time() - start_time) * 1000)
            
            return TestConnectionResponse(
                success=True,
                message="Connection successful",
                version=server_info.get('version', 'Unknown'),
                response_time_ms=response_time,
            )
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            response_time = int((time.time() - start_time) * 1000)
            return TestConnectionResponse(
                success=False,
                error=str(e),
                response_time_ms=response_time,
            )
        except Exception as e:
            response_time = int((time.time() - start_time) * 1000)
            return TestConnectionResponse(
                success=False,
                error=str(e),
                response_time_ms=response_time,
            )
    
    else:
        return TestConnectionResponse(
            success=False,
            error=f"Unsupported source type: {source_type}",
        )


@app.post("/connections", status_code=201)
async def create_or_update_connection(
    request: Dict[str, Any],
    token: Optional[str] = Depends(verify_token),
):
    """
    Create or update a connection for a data source.
    Validates the connection configuration and saves it to the database.
    """
    try:
        organization_id = request.get("organization_id")
        data_source_id = request.get("data_source_id")
        connection_type = request.get("connection_type")
        config = request.get("config") or {}
        
        # Validate required fields
        if not organization_id:
            raise HTTPException(status_code=400, detail="organization_id is required")
        if not data_source_id:
            raise HTTPException(status_code=400, detail="data_source_id is required")
        if not connection_type:
            raise HTTPException(status_code=400, detail="connection_type is required")
        if not config:
            raise HTTPException(status_code=400, detail="config is required")
        
        # Normalize connection type to match source type format
        normalized_type = normalize_source_type(connection_type)
        if normalized_type not in ['postgresql', 'mysql', 'mongodb']:
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported connection type: {connection_type}. Supported: postgresql, mysql, mongodb"
            )
        
        # Test the connection
        test_request_data = {
            "type": normalized_type,
        }
        
        # Map config fields to test request
        if config.get("connection_string"):
            test_request_data["connection_string"] = config["connection_string"]
        elif config.get("connection_string_mongo"):
            test_request_data["connection_string_mongo"] = config["connection_string_mongo"]
        else:
            test_request_data["host"] = config.get("host")
            test_request_data["port"] = config.get("port")
            test_request_data["database"] = config.get("database")
            test_request_data["username"] = config.get("username") or config.get("user")
            test_request_data["password"] = config.get("password")
            test_request_data["ssl"] = config.get("ssl")
            test_request_data["auth_source"] = config.get("auth_source")
            test_request_data["replica_set"] = config.get("replica_set")
            test_request_data["tls"] = config.get("tls")
        
        test_request = TestConnectionRequest(**test_request_data)
        test_result = await test_connection_internal(test_request)
        
        if not test_result.success:
            raise HTTPException(
                status_code=400,
                detail=f"Connection test failed: {test_result.error}"
            )
        
        # Get database URL from environment
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise HTTPException(
                status_code=500,
                detail="DATABASE_URL environment variable is not set. Cannot save connection to database."
            )
        
        # Remove quotes if present
        database_url = database_url.strip('"').strip("'")
        
        # Save connection to database
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            
            conn = psycopg2.connect(database_url)
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # Check if connection already exists for this data source
            cursor.execute("""
                SELECT id, data_source_id
                FROM data_source_connections
                WHERE data_source_id = %s
                LIMIT 1
            """, (data_source_id,))
            
            existing = cursor.fetchone()
            
            # Prepare config as JSON
            config_json = json.dumps(config) if not isinstance(config, str) else config
            
            if existing:
                # Update existing connection
                cursor.execute("""
                    UPDATE data_source_connections
                    SET 
                        connection_type = %s,
                        config = %s::jsonb,
                        status = %s,
                        updated_at = NOW()
                    WHERE data_source_id = %s
                    RETURNING 
                        id,
                        data_source_id,
                        connection_type,
                        config,
                        status,
                        last_connected_at,
                        last_error,
                        created_at,
                        updated_at
                """, (
                    normalized_type,
                    config_json,
                    'active' if test_result.success else 'inactive',
                    data_source_id,
                ))
                
                result = cursor.fetchone()
                conn.commit()
                cursor.close()
                conn.close()
                
                connection = dict(result)
                logger.info(f"Updated connection for data source {data_source_id}")
            else:
                # Create new connection
                connection_id = request.get("id") or str(uuid.uuid4())
                
                cursor.execute("""
                    INSERT INTO data_source_connections (
                        id,
                        data_source_id,
                        connection_type,
                        config,
                        status
                    ) VALUES (
                        %s, %s, %s, %s::jsonb, %s
                    )
                    RETURNING 
                        id,
                        data_source_id,
                        connection_type,
                        config,
                        status,
                        last_connected_at,
                        last_error,
                        created_at,
                        updated_at
                """, (
                    connection_id,
                    data_source_id,
                    normalized_type,
                    config_json,
                    'active' if test_result.success else 'inactive',
                ))
                
                result = cursor.fetchone()
                conn.commit()
                cursor.close()
                conn.close()
                
                connection = dict(result)
                logger.info(f"Created connection for data source {data_source_id}")
            
            # Parse config from JSON if needed
            connection_config = json.loads(connection['config']) if isinstance(connection['config'], str) else connection['config']
            
            # Return response matching frontend expectations
            return {
                "id": str(connection['id']),
                "data_source_id": str(connection['data_source_id']),
                "connection_type": connection['connection_type'],
                "config": connection_config,
                "status": connection['status'],
                "created_at": connection['created_at'].isoformat() if connection.get('created_at') else datetime.utcnow().isoformat(),
                "updated_at": connection['updated_at'].isoformat() if connection.get('updated_at') else datetime.utcnow().isoformat(),
            }
        
        except psycopg2.IntegrityError as e:
            if conn:
                conn.rollback()
                cursor.close()
                conn.close()
            logger.error(f"Database integrity error: {str(e)}", exc_info=True)
            raise HTTPException(status_code=400, detail=f"Database error: {str(e)}")
        except psycopg2.Error as e:
            if conn:
                conn.rollback()
                if cursor:
                    cursor.close()
                conn.close()
            logger.error(f"Database error: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create/update connection error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create/update connection: {str(e)}")


@app.post("/test-connection", response_model=TestConnectionResponse)
async def test_connection(
    request: TestConnectionRequest,
    token: Optional[str] = Depends(verify_token),
):
    """
    Test connection to a data source.
    Validates connection configuration and returns connection status.
    """
    try:
        return await test_connection_internal(request)
    except Exception as e:
        logger.error(f"Test connection error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Test connection failed: {str(e)}")


@app.get("/organizations/{organization_id}/data-sources/{data_source_id}/connection")
async def get_connection(
    organization_id: str,
    data_source_id: str,
    includeSensitive: bool = False,
    token: Optional[str] = Depends(verify_token),
):
    """
    Get connection details for a data source.
    Fetches from Supabase database.
    """
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        # Get database URL from environment
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise HTTPException(
                status_code=500,
                detail="DATABASE_URL environment variable is not set"
            )
        
        # Remove quotes if present
        database_url = database_url.strip('"').strip("'")
        
        # Connect to database and fetch connection
        try:
            conn = psycopg2.connect(database_url)
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # First check if data source exists and belongs to organization
            cursor.execute("""
                SELECT id, source_type, name, organization_id
                FROM data_sources
                WHERE id = %s AND organization_id = %s AND deleted_at IS NULL
            """, (data_source_id, organization_id))
            
            data_source = cursor.fetchone()
            if not data_source:
                cursor.close()
                conn.close()
                logger.warning(f"Data source {data_source_id} not found or does not belong to organization {organization_id}")
                return None
            
            # Fetch connection from data_source_connections table
            cursor.execute("""
                SELECT 
                    dsc.id,
                    dsc.data_source_id,
                    dsc.connection_type,
                    dsc.config,
                    dsc.status,
                    dsc.last_connected_at,
                    dsc.last_error,
                    dsc.created_at,
                    dsc.updated_at
                FROM data_source_connections dsc
                JOIN data_sources ds ON ds.id = dsc.data_source_id
                WHERE dsc.data_source_id = %s AND ds.organization_id = %s
                ORDER BY dsc.updated_at DESC
                LIMIT 1
            """, (data_source_id, organization_id))
            
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if not result:
                logger.warning(f"No connection found for data source {data_source_id}")
                return None
            
            connection = dict(result)
            
            # Decrypt config if includeSensitive is True
            # For now, config is stored as JSON, but in production it should be encrypted
            config = json.loads(connection['config']) if isinstance(connection['config'], str) else connection['config']
            
            if not includeSensitive:
                # Mask sensitive fields
                if isinstance(config, dict):
                    if 'password' in config:
                        config['password'] = '***'
                    if 'connection_string' in config and 'password' in config['connection_string']:
                        # Mask password in connection string
                        import re
                        config['connection_string'] = re.sub(
                            r':([^:@]+)@',
                            r':***@',
                            config['connection_string']
                        )
            
            return {
                "id": str(connection['id']),
                "data_source_id": str(connection['data_source_id']),
                "connection_type": connection['connection_type'],
                "config": config,
                "status": connection['status'],
                "last_connected_at": connection['last_connected_at'].isoformat() if connection.get('last_connected_at') else None,
                "last_error": connection.get('last_error'),
                "created_at": connection['created_at'].isoformat() if connection.get('created_at') else None,
                "updated_at": connection['updated_at'].isoformat() if connection.get('updated_at') else None,
            }
        
        except psycopg2.Error as e:
            logger.error(f"Database error: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Get connection error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to get connection: {str(e)}")


# ============================================================================
# Connector Discovery Helpers
# ============================================================================

async def discover_postgres_schemas(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Discover schemas and tables using tap-postgres connector."""
    # Build connection config for tap-postgres
    conn_config = {
        'host': config.get('host'),
        'port': config.get('port', 5432),
        'user': config.get('username') or config.get('user'),
        'password': config.get('password'),
        'dbname': config.get('database') or config.get('dbname'),
    }
    
    if config.get('ssl') and config['ssl'].get('enabled'):
        conn_config['sslmode'] = 'require'
    
    # Use tap-postgres discovery
    # Import here after sys.path is set up
    from contextlib import redirect_stdout
    
    # Capture stdout since do_discovery writes to stdout (it calls dump_catalog)
    f = io.StringIO()
    with redirect_stdout(f):
        from tap_postgres import do_discovery
        streams = do_discovery(conn_config)
    
    # Group streams by schema
    schemas_map = {}
    
    for stream in streams:
        # Extract schema name from metadata
        metadata_map = {}
        for md in stream.get('metadata', []):
            breadcrumb = md.get('breadcrumb', [])
            if not breadcrumb:  # Root metadata
                metadata_map[()] = md.get('metadata', {})
        
        root_metadata = metadata_map.get((), {})
        schema_name = root_metadata.get('schema-name', 'public')
        table_name = root_metadata.get('table-name') or stream.get('stream', '')
        
        # Get row count and is_view
        row_count = root_metadata.get('row-count')
        is_view = root_metadata.get('is-view', False)
        
        # Get primary keys
        table_key_properties = root_metadata.get('table-key-properties', [])
        
        # Get columns from schema
        columns = []
        schema_props = stream.get('schema', {}).get('properties', {})
        for col_name, col_schema in schema_props.items():
            col_type = col_schema.get('type', 'string')
            if isinstance(col_type, list):
                col_type = col_type[0] if col_type else 'string'
            
            columns.append({
                'name': col_name,
                'type': col_type,
                'nullable': 'null' in col_type if isinstance(col_type, list) else False,
            })
        
        # Initialize schema if not exists
        if schema_name not in schemas_map:
            schemas_map[schema_name] = {
                'name': schema_name,
                'tables': []
            }
        
        # Add table
        schemas_map[schema_name]['tables'].append({
            'name': table_name,
            'schema': schema_name,
            'type': 'view' if is_view else 'table',
            'rowCount': row_count,
            'columns': columns,
            'primaryKeys': table_key_properties,
        })
    
    return list(schemas_map.values())


async def discover_mysql_schemas(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Discover schemas and tables using tap-mysql connector."""
    try:
        import pymysql
    except ImportError:
        try:
            import PyMySQL as pymysql
        except ImportError:
            raise HTTPException(
                status_code=500,
                detail="MySQL driver not installed. Install with: pip install pymysql"
            )
    
    from tap_mysql import discover_catalog
    from tap_mysql.connection import connect_with_backoff, MySQLConnection
    
    # Build connection config for tap-mysql
    mysql_config = {
        'host': config.get('host'),
        'port': config.get('port', 3306),
        'user': config.get('username') or config.get('user'),
        'password': config.get('password'),
        'database': config.get('database'),
    }
    
    # Create MySQL connection and use tap-mysql discovery
    mysql_conn = MySQLConnection(mysql_config)
    connect_with_backoff(mysql_conn)
    
    try:
        # Use tap-mysql discovery
        catalog = discover_catalog(mysql_conn, mysql_config)
        
        # Group streams by schema (database)
        schemas_map = {}
        
        for entry in catalog.streams:
            # Extract metadata
            from singer import metadata
            md_map = metadata.to_map(entry.metadata)
            root_md = md_map.get((), {})
            
            database_name = root_md.get('database-name', '')
            table_name = entry.table
            is_view = root_md.get('is-view', False)
            row_count = root_md.get('row-count')
            table_key_properties = root_md.get('table-key-properties', [])
            
            # Get columns
            columns = []
            for col_name, col_schema in entry.schema.properties.items():
                col_type = col_schema.type if hasattr(col_schema, 'type') else 'string'
                columns.append({
                    'name': col_name,
                    'type': col_type,
                    'nullable': True,  # MySQL doesn't always expose this in schema
                })
            
            # Initialize schema if not exists
            if database_name not in schemas_map:
                schemas_map[database_name] = {
                    'name': database_name,
                    'tables': []
                }
            
            # Add table
            schemas_map[database_name]['tables'].append({
                'name': table_name,
                'schema': database_name,
                'type': 'view' if is_view else 'table',
                'rowCount': row_count,
                'columns': columns,
                'primaryKeys': table_key_properties,
            })
        
        return list(schemas_map.values())
    finally:
        if mysql_conn:
            mysql_conn.close()


async def discover_mongodb_schemas(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Discover databases and collections using tap-mongodb connector."""
    from pymongo import MongoClient
    from tap_mongodb import get_databases, produce_collection_schema
    
    # Build connection config
    connection_params = {
        "host": config.get('host'),
        "port": int(config.get('port', 27017)),
        "username": config.get('username') or config.get('user'),
        "password": config.get('password'),
        "authSource": config.get('database') or config.get('auth_source', 'admin'),
    }
    
    if config.get('ssl') == 'true' or config.get('tls'):
        connection_params["ssl"] = True
    
    if config.get('replica_set'):
        connection_params["replicaset"] = config['replica_set']
    
    # Create MongoDB client
    client = MongoClient(**connection_params)
    
    try:
        # Get databases
        db_names = get_databases(client, config)
        
        # Group collections by database
        schemas_map = {}
        
        for db_name in db_names:
            db = client[db_name]
            collection_names = db.list_collection_names()
            
            for collection_name in [c for c in collection_names if not c.startswith("system.")]:
                collection = db[collection_name]
                is_view = collection.options().get('viewOn') is not None
                
                if is_view:
                    continue  # Skip views for now
                
                # Use tap-mongodb to produce collection schema
                stream = produce_collection_schema(collection)
                
                # Extract metadata
                from singer import metadata
                md_map = metadata.to_map(stream['metadata'])
                root_md = md_map.get((), {})
                
                database_name = root_md.get('database-name', db_name)
                row_count = root_md.get('row-count')
                table_key_properties = root_md.get('table-key-properties', ['_id'])
                
                # Initialize schema if not exists
                if database_name not in schemas_map:
                    schemas_map[database_name] = {
                        'name': database_name,
                        'tables': []
                    }
                
                # Add collection (as table)
                schemas_map[database_name]['tables'].append({
                    'name': collection_name,
                    'schema': database_name,
                    'type': 'collection',
                    'rowCount': row_count,
                    'columns': [],  # MongoDB is schema-less, columns discovered at query time
                    'primaryKeys': table_key_properties,
                })
        
        return list(schemas_map.values())
    finally:
        client.close()


@app.get("/organizations/{organization_id}/data-sources/{data_source_id}/schemas")
async def list_schemas_with_tables(
    organization_id: str,
    data_source_id: str,
    token: Optional[str] = Depends(verify_token),
):
    """
    List all schemas and tables for a data source.
    Uses tap-postgres discovery to get schemas and tables.
    """
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        # Get database URL from environment
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise HTTPException(
                status_code=500,
                detail="DATABASE_URL environment variable is not set"
            )
        
        # Remove quotes if present
        database_url = database_url.strip('"').strip("'")
        
        # First, get connection config
        try:
            conn = psycopg2.connect(database_url)
            cursor = conn.cursor(cursor_factory=RealDictCursor)
            
            # First check if data source exists and belongs to organization
            cursor.execute("""
                SELECT id, source_type, name, organization_id
                FROM data_sources
                WHERE id = %s AND organization_id = %s AND deleted_at IS NULL
            """, (data_source_id, organization_id))
            
            data_source = cursor.fetchone()
            if not data_source:
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404,
                    detail=f"Data source {data_source_id} not found or does not belong to organization {organization_id}"
                )
            
            # Fetch connection (with organization check)
            cursor.execute("""
                SELECT 
                    dsc.connection_type,
                    dsc.config,
                    dsc.status,
                    ds.source_type
                FROM data_source_connections dsc
                JOIN data_sources ds ON ds.id = dsc.data_source_id
                WHERE dsc.data_source_id = %s AND ds.organization_id = %s
                ORDER BY dsc.updated_at DESC
                LIMIT 1
            """, (data_source_id, organization_id))
            
            result = cursor.fetchone()
            
            if not result:
                # Log more details for debugging (before closing cursor)
                cursor.execute("""
                    SELECT COUNT(*) as count
                    FROM data_source_connections
                    WHERE data_source_id = %s
                """, (data_source_id,))
                count_result = cursor.fetchone()
                count = count_result['count'] if count_result else 0
                cursor.close()
                conn.close()
                
                logger.warning(
                    f"No connection found for data source {data_source_id}. "
                    f"Total connections for this data_source_id: {count}"
                )
                raise HTTPException(
                    status_code=404,
                    detail=f"Connection not configured for data source {data_source_id}. Please configure the connection first by going to the data source settings and adding connection credentials."
                )
            
            cursor.close()
            conn.close()
            
            connection = dict(result)
            config = json.loads(connection['config']) if isinstance(connection['config'], str) else connection['config']
            source_type = connection.get('source_type') or connection.get('connection_type')
            
            logger.info(f"Found connection for data source {data_source_id}, type: {source_type}")
            
        except psycopg2.Error as e:
            logger.error(f"Database error: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
        
        # Normalize source type
        normalized_type = normalize_source_type(source_type)
        
        # Use connector-specific discovery
        try:
            if normalized_type in ['postgresql', 'postgres']:
                schemas = await discover_postgres_schemas(config)
            elif normalized_type == 'mysql':
                schemas = await discover_mysql_schemas(config)
            elif normalized_type == 'mongodb':
                schemas = await discover_mongodb_schemas(config)
            else:
                raise HTTPException(
                    status_code=400,
                    detail=f"Schema discovery for {source_type} not yet implemented. Supported: postgresql, mysql, mongodb"
                )
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Discovery failed for {normalized_type}: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=500,
                detail=f"Failed to discover schemas: {str(e)}"
            )
        
        return {
            'schemas': schemas,
            'type': normalized_type,
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"List schemas error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to list schemas: {str(e)}")


# ============================================================================
# Pipeline Management Endpoints
# ============================================================================

@app.post("/organizations/{organization_id}/pipelines", status_code=201)
async def create_pipeline(
    organization_id: str,
    request: Dict[str, Any],
    token: Optional[str] = Depends(verify_token),
):
    """
    Create a new pipeline with source and destination schemas.
    Python API handles the complete pipeline creation flow.
    """
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        # Get database URL from environment
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise HTTPException(
                status_code=500,
                detail="DATABASE_URL environment variable is not set"
            )
        
        # Remove quotes if present
        database_url = database_url.strip('"').strip("'")
        
        # Extract request data
        name = request.get("name")
        description = request.get("description")
        source_schema_data = request.get("source_schema")
        destination_schema_data = request.get("destination_schema")
        sync_mode = request.get("sync_mode", "full")
        sync_frequency = request.get("sync_frequency", "manual")
        incremental_column = request.get("incremental_column")
        schedule_type = request.get("schedule_type", "none")
        schedule_value = request.get("schedule_value")
        schedule_timezone = request.get("schedule_timezone", "UTC")
        transformations = request.get("transformations", [])
        
        # Validate required fields
        if not name:
            raise HTTPException(status_code=400, detail="name is required")
        if not source_schema_data:
            raise HTTPException(status_code=400, detail="source_schema is required")
        if not destination_schema_data:
            raise HTTPException(status_code=400, detail="destination_schema is required")
        
        # Get user ID from token
        created_by = None
        if token:
            token_data = decode_jwt_token(token)
            if token_data and token_data.get("sub"):
                created_by = token_data.get("sub")
        
        if not created_by:
            created_by = request.get("created_by")
        
        if not created_by:
            raise HTTPException(
                status_code=400,
                detail="created_by (user ID) is required. Provide in request or use valid JWT token."
            )
        
        # Connect to database
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # Step 1: Create source schema
            source_schema_id = str(uuid.uuid4())
            source_schema_insert = {
                'id': source_schema_id,
                'organization_id': organization_id,
                'source_type': source_schema_data.get('source_type'),
                'data_source_id': source_schema_data.get('data_source_id'),
                'source_schema': source_schema_data.get('source_schema'),
                'source_table': source_schema_data.get('source_table'),
                'source_query': source_schema_data.get('source_query'),
                'name': source_schema_data.get('name'),
                'is_active': source_schema_data.get('is_active', True),
            }
            
            cursor.execute("""
                INSERT INTO pipeline_source_schemas (
                    id, organization_id, source_type, data_source_id,
                    source_schema, source_table, source_query, name, is_active,
                    created_at, updated_at
                ) VALUES (
                    %(id)s, %(organization_id)s, %(source_type)s, %(data_source_id)s,
                    %(source_schema)s, %(source_table)s, %(source_query)s, %(name)s, %(is_active)s,
                    NOW(), NOW()
                )
                RETURNING id, organization_id, source_type, data_source_id,
                          source_schema, source_table, source_query, name,
                          is_active, created_at, updated_at
            """, source_schema_insert)
            
            source_schema_result = cursor.fetchone()
            
            # Step 2: Create destination schema
            destination_schema_id = str(uuid.uuid4())
            destination_schema_insert = {
                'id': destination_schema_id,
                'organization_id': organization_id,
                'data_source_id': destination_schema_data.get('data_source_id'),
                'destination_schema': destination_schema_data.get('destination_schema', 'public'),
                'destination_table': destination_schema_data.get('destination_table'),
                'transform_script': destination_schema_data.get('transform_script'),
                'write_mode': destination_schema_data.get('write_mode', 'append'),
                'upsert_key': json.dumps(destination_schema_data.get('upsert_key', [])) if destination_schema_data.get('upsert_key') else None,
                'name': destination_schema_data.get('name'),
                'is_active': destination_schema_data.get('is_active', True),
            }
            
            cursor.execute("""
                INSERT INTO pipeline_destination_schemas (
                    id, organization_id, data_source_id,
                    destination_schema, destination_table, transform_script,
                    write_mode, upsert_key, name, is_active,
                    created_at, updated_at
                ) VALUES (
                    %(id)s, %(organization_id)s, %(data_source_id)s,
                    %(destination_schema)s, %(destination_table)s, %(transform_script)s,
                    %(write_mode)s, %(upsert_key)s::jsonb, %(name)s, %(is_active)s,
                    NOW(), NOW()
                )
                RETURNING id, organization_id, data_source_id,
                          destination_schema, destination_table, transform_script,
                          write_mode, upsert_key, name, is_active,
                          created_at, updated_at
            """, destination_schema_insert)
            
            destination_schema_result = cursor.fetchone()
            
            # Step 3: Create pipeline
            pipeline_id = str(uuid.uuid4())
            pipeline_insert = {
                'id': pipeline_id,
                'organization_id': organization_id,
                'created_by': created_by,
                'name': name,
                'description': description,
                'source_schema_id': source_schema_id,
                'destination_schema_id': destination_schema_id,
                'transformations': json.dumps(transformations) if transformations else None,
                'sync_mode': sync_mode,
                'incremental_column': incremental_column,
                'sync_frequency': sync_frequency,
                'schedule_type': schedule_type,
                'schedule_value': schedule_value,
                'schedule_timezone': schedule_timezone,
                'status': 'idle',
            }
            
            # Calculate initial next_scheduled_run_at based on schedule
            # For CDC/incremental pipelines, schedule first run immediately
            next_run_sql = "NULL"
            if schedule_type == 'minutes' and schedule_value:
                # Set next run to NOW + X minutes (but start immediately for first run)
                next_run_sql = "NOW()"  # Start immediately, then schedule subsequent runs
            elif schedule_type != 'none' and schedule_type:
                next_run_sql = "NOW()"  # Start immediately for any scheduled pipeline
            elif sync_mode == 'incremental':
                next_run_sql = "NOW()"  # Auto-start incremental pipelines
            
            cursor.execute(f"""
                INSERT INTO pipelines (
                    id, organization_id, created_by, name, description,
                    source_schema_id, destination_schema_id, transformations,
                    sync_mode, incremental_column, sync_frequency,
                    schedule_type, schedule_value, schedule_timezone, status,
                    next_scheduled_run_at, next_sync_at,
                    created_at, updated_at
                ) VALUES (
                    %(id)s, %(organization_id)s, %(created_by)s, %(name)s, %(description)s,
                    %(source_schema_id)s, %(destination_schema_id)s, %(transformations)s::jsonb,
                    %(sync_mode)s, %(incremental_column)s, %(sync_frequency)s,
                    %(schedule_type)s, %(schedule_value)s, %(schedule_timezone)s, %(status)s,
                    {next_run_sql}, {next_run_sql},
                    NOW(), NOW()
                )
                RETURNING id, organization_id, created_by, name, description,
                          source_schema_id, destination_schema_id, transformations,
                          sync_mode, incremental_column, sync_frequency,
                          schedule_type, schedule_value, schedule_timezone, status,
                          next_scheduled_run_at, next_sync_at,
                          created_at, updated_at, deleted_at
            """, pipeline_insert)
            
            pipeline_result = cursor.fetchone()
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Format response
            pipeline_dict = dict(pipeline_result)
            
            return {
                "id": str(pipeline_dict['id']),
                "organizationId": str(pipeline_dict['organization_id']),
                "createdBy": str(pipeline_dict['created_by']),
                "name": pipeline_dict['name'],
                "description": pipeline_dict.get('description'),
                "sourceSchemaId": str(pipeline_dict['source_schema_id']),
                "destinationSchemaId": str(pipeline_dict['destination_schema_id']),
                "transformations": json.loads(pipeline_dict['transformations']) if pipeline_dict.get('transformations') else None,
                "syncMode": pipeline_dict.get('sync_mode', 'full'),
                "incrementalColumn": pipeline_dict.get('incremental_column'),
                "syncFrequency": pipeline_dict.get('sync_frequency', 'manual'),
                "scheduleType": pipeline_dict.get('schedule_type', 'none'),
                "scheduleValue": pipeline_dict.get('schedule_value'),
                "scheduleTimezone": pipeline_dict.get('schedule_timezone', 'UTC'),
                "status": pipeline_dict.get('status', 'idle'),
                "nextScheduledRunAt": pipeline_dict['next_scheduled_run_at'].isoformat() if pipeline_dict.get('next_scheduled_run_at') else None,
                "nextSyncAt": pipeline_dict['next_sync_at'].isoformat() if pipeline_dict.get('next_sync_at') else None,
                "createdAt": pipeline_dict['created_at'].isoformat() if pipeline_dict.get('created_at') else None,
                "updatedAt": pipeline_dict['updated_at'].isoformat() if pipeline_dict.get('updated_at') else None,
                "deletedAt": pipeline_dict['deleted_at'].isoformat() if pipeline_dict.get('deleted_at') else None,
            }
        
        except psycopg2.IntegrityError as e:
            conn.rollback()
            cursor.close()
            conn.close()
            logger.error(f"Database integrity error: {str(e)}", exc_info=True)
            raise HTTPException(status_code=400, detail=f"Database error: {str(e)}")
        except psycopg2.Error as e:
            conn.rollback()
            cursor.close()
            conn.close()
            logger.error(f"Database error: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Create pipeline error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to create pipeline: {str(e)}")


@app.patch("/organizations/{organization_id}/pipelines/{pipeline_id}")
async def update_pipeline(
    organization_id: str,
    pipeline_id: str,
    request: Dict[str, Any],
    token: Optional[str] = Depends(verify_token),
):
    """
    Update an existing pipeline.
    Python API handles pipeline updates.
    """
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        
        # Get database URL from environment
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise HTTPException(
                status_code=500,
                detail="DATABASE_URL environment variable is not set"
            )
        
        # Remove quotes if present
        database_url = database_url.strip('"').strip("'")
        
        # Connect to database
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # First verify pipeline exists and belongs to organization
            cursor.execute("""
                SELECT id, organization_id, name
                FROM pipelines
                WHERE id = %s AND organization_id = %s AND deleted_at IS NULL
            """, (pipeline_id, organization_id))
            
            existing_pipeline = cursor.fetchone()
            if not existing_pipeline:
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404,
                    detail=f"Pipeline {pipeline_id} not found or does not belong to organization {organization_id}"
                )
            
            # Build update query dynamically based on provided fields
            update_fields = []
            update_values = []
            
            if "name" in request:
                update_fields.append("name = %s")
                update_values.append(request["name"])
            
            if "description" in request:
                update_fields.append("description = %s")
                update_values.append(request["description"])
            
            if "sync_mode" in request:
                update_fields.append("sync_mode = %s")
                update_values.append(request["sync_mode"])
            
            if "incremental_column" in request:
                update_fields.append("incremental_column = %s")
                update_values.append(request["incremental_column"])
            
            if "sync_frequency" in request:
                update_fields.append("sync_frequency = %s")
                update_values.append(request["sync_frequency"])
            
            if "schedule_type" in request:
                update_fields.append("schedule_type = %s")
                update_values.append(request["schedule_type"])
            
            if "schedule_value" in request:
                update_fields.append("schedule_value = %s")
                update_values.append(request["schedule_value"])
            
            if "schedule_timezone" in request:
                update_fields.append("schedule_timezone = %s")
                update_values.append(request["schedule_timezone"])
            
            if "transformations" in request:
                update_fields.append("transformations = %s::jsonb")
                update_values.append(json.dumps(request["transformations"]) if request["transformations"] else None)
            
            if not update_fields:
                cursor.close()
                conn.close()
                raise HTTPException(status_code=400, detail="No fields to update")
            
            # Add updated_at and pipeline_id for WHERE clause
            update_fields.append("updated_at = NOW()")
            update_values.extend([pipeline_id, organization_id])
            
            # Execute update
            update_query = f"""
                UPDATE pipelines
                SET {', '.join(update_fields)}
                WHERE id = %s AND organization_id = %s AND deleted_at IS NULL
                RETURNING id, organization_id, created_by, name, description,
                          source_schema_id, destination_schema_id, transformations,
                          sync_mode, incremental_column, sync_frequency,
                          schedule_type, schedule_value, schedule_timezone, status,
                          created_at, updated_at, deleted_at
            """
            
            cursor.execute(update_query, update_values)
            result = cursor.fetchone()
            
            if not result:
                cursor.close()
                conn.close()
                raise HTTPException(
                    status_code=404,
                    detail=f"Pipeline {pipeline_id} not found or does not belong to organization {organization_id}"
                )
            
            conn.commit()
            cursor.close()
            conn.close()
            
            # Format response
            pipeline_dict = dict(result)
            
            return {
                "id": str(pipeline_dict['id']),
                "organizationId": str(pipeline_dict['organization_id']),
                "createdBy": str(pipeline_dict['created_by']),
                "name": pipeline_dict['name'],
                "description": pipeline_dict.get('description'),
                "sourceSchemaId": str(pipeline_dict['source_schema_id']),
                "destinationSchemaId": str(pipeline_dict['destination_schema_id']),
                "transformations": json.loads(pipeline_dict['transformations']) if pipeline_dict.get('transformations') else None,
                "syncMode": pipeline_dict.get('sync_mode', 'full'),
                "incrementalColumn": pipeline_dict.get('incremental_column'),
                "syncFrequency": pipeline_dict.get('sync_frequency', 'manual'),
                "scheduleType": pipeline_dict.get('schedule_type', 'none'),
                "scheduleValue": pipeline_dict.get('schedule_value'),
                "scheduleTimezone": pipeline_dict.get('schedule_timezone', 'UTC'),
                "status": pipeline_dict.get('status', 'idle'),
                "createdAt": pipeline_dict['created_at'].isoformat() if pipeline_dict.get('created_at') else None,
                "updatedAt": pipeline_dict['updated_at'].isoformat() if pipeline_dict.get('updated_at') else None,
                "deletedAt": pipeline_dict['deleted_at'].isoformat() if pipeline_dict.get('deleted_at') else None,
            }
        
        except psycopg2.IntegrityError as e:
            conn.rollback()
            cursor.close()
            conn.close()
            logger.error(f"Database integrity error: {str(e)}", exc_info=True)
            raise HTTPException(status_code=400, detail=f"Database error: {str(e)}")
        except psycopg2.Error as e:
            conn.rollback()
            cursor.close()
            conn.close()
            logger.error(f"Database error: {str(e)}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Update pipeline error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to update pipeline: {str(e)}")


# ============================================================================
# Pipeline Run/Execution Endpoints (Direct, no NestJS proxy)
# ============================================================================

class PipelineRunRequest(BaseModel):
    """Request model for running a pipeline"""
    sync_mode: Optional[str] = 'full'  # 'full' or 'incremental'
    limit: Optional[int] = None  # Limit rows per batch
    
    model_config = ConfigDict(extra='allow')


@app.post("/organizations/{organization_id}/pipelines/{pipeline_id}/run")
async def run_pipeline(
    organization_id: str,
    pipeline_id: str,
    request: Optional[PipelineRunRequest] = None,
    token: Optional[str] = Depends(verify_token),
):
    """
    Run a pipeline directly from Python API.
    This bypasses NestJS proxy to avoid timeout issues.
    
    Steps:
    1. Get pipeline with source/destination schemas
    2. Get connection configs for source and destination
    3. Collect data from source using Singer taps
    4. Transform data using user script
    5. Emit to destination
    6. Update pipeline status
    """
    import psycopg2
    from psycopg2.extras import RealDictCursor
    
    if request is None:
        request = PipelineRunRequest()
    
    try:
        # Get database URL
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise HTTPException(status_code=500, detail="DATABASE_URL not configured")
        
        database_url = database_url.strip('"').strip("'")
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        try:
            # =====================================================
            # Step 1: Get pipeline with schemas
            # =====================================================
            cursor.execute("""
                SELECT 
                    p.id, p.organization_id, p.name, p.description,
                    p.source_schema_id, p.destination_schema_id,
                    p.transformations, p.sync_mode, p.status,
                    -- Source schema
                    ss.data_source_id AS source_data_source_id,
                    ss.source_type, ss.source_schema, ss.source_table,
                    ss.source_query, ss.source_config,
                    -- Destination schema
                    ds.data_source_id AS dest_data_source_id,
                    ds.destination_schema, ds.destination_table,
                    ds.write_mode, ds.primary_keys, ds.transform_script
                FROM pipelines p
                LEFT JOIN pipeline_source_schemas ss ON p.source_schema_id = ss.id
                LEFT JOIN pipeline_destination_schemas ds ON p.destination_schema_id = ds.id
                WHERE p.id = %s AND p.organization_id = %s AND p.deleted_at IS NULL
            """, (pipeline_id, organization_id))
            
            pipeline = cursor.fetchone()
            if not pipeline:
                raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")
            
            # =====================================================
            # Step 2: Get connection configs
            # =====================================================
            # Source connection
            cursor.execute("""
                SELECT config, connection_type
                FROM data_source_connections
                WHERE data_source_id = %s
            """, (pipeline['source_data_source_id'],))
            source_conn = cursor.fetchone()
            if not source_conn:
                raise HTTPException(status_code=400, detail="Source connection not configured")
            
            # Destination connection
            cursor.execute("""
                SELECT config, connection_type
                FROM data_source_connections
                WHERE data_source_id = %s
            """, (pipeline['dest_data_source_id'],))
            dest_conn = cursor.fetchone()
            if not dest_conn:
                raise HTTPException(status_code=400, detail="Destination connection not configured")
            
            # =====================================================
            # Step 3: Create pipeline run record
            # =====================================================
            run_id = str(uuid.uuid4())
            cursor.execute("""
                INSERT INTO pipeline_runs (
                    id, pipeline_id, organization_id, status, trigger_type, started_at
                ) VALUES (%s, %s, %s, 'running', 'manual', NOW())
                RETURNING id
            """, (run_id, pipeline_id, organization_id))
            conn.commit()
            
            # Update pipeline status to running
            cursor.execute("""
                UPDATE pipelines SET status = 'running', updated_at = NOW()
                WHERE id = %s
            """, (pipeline_id,))
            conn.commit()
            
        finally:
            cursor.close()
            conn.close()
        
        # =====================================================
        # Step 4: Execute pipeline (collect -> transform -> emit)
        # =====================================================
        try:
            # Parse connection configs if they're JSON strings
            source_config_raw = source_conn['config']
            dest_config_raw = dest_conn['config']
            
            if isinstance(source_config_raw, str):
                source_config_raw = json.loads(source_config_raw)
            if isinstance(dest_config_raw, str):
                dest_config_raw = json.loads(dest_config_raw)
            
            # Decrypt connection configs
            source_config = decrypt_connection_config(source_conn['connection_type'], source_config_raw)
            dest_config = decrypt_connection_config(dest_conn['connection_type'], dest_config_raw)
            
            # Prepare collect request
            sync_mode = request.sync_mode or pipeline.get('sync_mode') or 'full'
            batch_size = request.limit or 500
            
            total_rows_read = 0
            total_rows_written = 0
            total_rows_skipped = 0
            total_rows_failed = 0
            all_errors = []
            
            offset = 0
            has_more = True
            
            while has_more:
                # Collect data
                logger.info(f"[Pipeline {pipeline_id}] Collecting batch at offset {offset}")
                
                # Get source type from pipeline or source connection
                source_type = pipeline.get('source_type') or source_conn['connection_type']
                
                collect_request = CollectRequest(
                    source_type=source_type,
                    connection_config=source_config,
                    source_config=pipeline.get('source_config') or {},
                    table_name=pipeline['source_table'],
                    schema_name=pipeline.get('source_schema') or 'public',
                    query=pipeline.get('source_query'),
                    sync_mode=sync_mode,
                    limit=batch_size,
                    offset=offset,
                )
                
                # Call collect endpoint internally
                collect_response = await collect(
                    source_type=source_type,
                    request=collect_request,
                    token=token,
                )
                
                rows = collect_response.rows
                total_rows_read += len(rows)
                
                if not rows:
                    logger.info(f"[Pipeline {pipeline_id}] No more rows to process")
                    break
                
                logger.info(f"[Pipeline {pipeline_id}] Collected {len(rows)} rows")
                
                # Transform data
                # First check destination schema's transform_script, then fall back to transformations
                transform_script = pipeline.get('transform_script')
                
                if not transform_script:
                    transformations = pipeline.get('transformations')
                    if transformations and isinstance(transformations, list) and len(transformations) > 0:
                        # Get first transformer's script
                        for t in transformations:
                            if t.get('script'):
                                transform_script = t['script']
                                break
                
                if transform_script:
                    logger.info(f"[Pipeline {pipeline_id}] Transforming {len(rows)} rows")
                    transform_request = TransformRequest(
                        rows=rows,
                        transform_script=transform_script,
                    )
                    transform_response = await transform(
                        request=transform_request,
                        token=token,
                    )
                    transformed_rows = transform_response.transformed_rows
                    if transform_response.errors:
                        all_errors.extend(transform_response.errors)
                else:
                    # No transformation, pass through
                    transformed_rows = rows
                
                if not transformed_rows:
                    logger.warning(f"[Pipeline {pipeline_id}] No rows after transformation")
                    offset += batch_size
                    has_more = collect_response.has_more
                    continue
                
                logger.info(f"[Pipeline {pipeline_id}] Emitting {len(transformed_rows)} rows")
                
                # Emit data
                write_mode = pipeline.get('write_mode') or 'append'
                # primary_keys is JSONB array, not a single string
                primary_keys = pipeline.get('primary_keys')
                if primary_keys:
                    upsert_key = primary_keys if isinstance(primary_keys, list) else [primary_keys]
                else:
                    upsert_key = None
                
                # Use destination connection type as dest_type
                dest_type = dest_conn['connection_type']
                
                emit_request = EmitRequest(
                    destination_type=dest_type,
                    connection_config=dest_config,
                    destination_config={},
                    table_name=pipeline['destination_table'],
                    schema_name=pipeline.get('destination_schema') or 'public',
                    rows=transformed_rows,
                    write_mode=write_mode,
                    upsert_key=upsert_key,
                )
                
                emit_response = await emit(
                    dest_type=dest_type,
                    request=emit_request,
                    token=token,
                )
                
                total_rows_written += emit_response.rows_written
                total_rows_skipped += emit_response.rows_skipped
                total_rows_failed += emit_response.rows_failed
                if emit_response.errors:
                    all_errors.extend(emit_response.errors[:5])  # Limit errors
                
                logger.info(f"[Pipeline {pipeline_id}] Batch complete: {emit_response.rows_written} written, {emit_response.rows_skipped} skipped")
                
                # Update for next batch
                offset += batch_size
                has_more = collect_response.has_more
            
            # =====================================================
            # Step 5: Update pipeline run status
            # =====================================================
            conn = psycopg2.connect(database_url)
            cursor = conn.cursor()
            
            try:
                # pipeline_runs.status uses 'run_status' enum: pending, running, success, failed, cancelled
                cursor.execute("""
                    UPDATE pipeline_runs
                    SET status = 'success',
                        completed_at = NOW(),
                        rows_read = %s,
                        rows_written = %s,
                        rows_skipped = %s,
                        rows_failed = %s,
                        duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at)),
                        updated_at = NOW()
                    WHERE id = %s
                """, (total_rows_read, total_rows_written, total_rows_skipped, total_rows_failed, run_id))
                
                # pipelines.status uses 'pipeline_status' enum: idle, running, paused, failed, completed
                # Set to 'idle' after completion so it can be scheduled again
                cursor.execute("""
                    UPDATE pipelines SET status = 'idle', last_run_status = 'success', updated_at = NOW()
                    WHERE id = %s
                """, (pipeline_id,))
                
                conn.commit()
            finally:
                cursor.close()
                conn.close()
            
            return {
                "success": True,
                "runId": run_id,
                "pipelineId": pipeline_id,
                "status": "success",
                "rowsRead": total_rows_read,
                "rowsWritten": total_rows_written,
                "rowsSkipped": total_rows_skipped,
                "rowsFailed": total_rows_failed,
                "errors": all_errors[:10] if all_errors else [],
            }
            
        except Exception as e:
            # Update pipeline run status to failed
            logger.error(f"[Pipeline {pipeline_id}] Execution failed: {str(e)}", exc_info=True)
            
            try:
                conn = psycopg2.connect(database_url)
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE pipeline_runs
                    SET status = 'failed',
                        completed_at = NOW(),
                        error_message = %s,
                        duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at)),
                        updated_at = NOW()
                    WHERE id = %s
                """, (str(e)[:1000], run_id))
                
                cursor.execute("""
                    UPDATE pipelines SET status = 'failed', updated_at = NOW()
                    WHERE id = %s
                """, (pipeline_id,))
                
                conn.commit()
                cursor.close()
                conn.close()
            except:
                pass
            
            raise HTTPException(status_code=500, detail=f"Pipeline execution failed: {str(e)}")
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Run pipeline error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to run pipeline: {str(e)}")


def decrypt_connection_config(connection_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Decrypt sensitive fields in connection config.
    Uses the same encryption key as NestJS.
    """
    from cryptography.fernet import Fernet
    import base64
    import hashlib
    
    encryption_key = os.getenv("ENCRYPTION_KEY")
    if not encryption_key:
        logger.warning("ENCRYPTION_KEY not set, returning config as-is")
        return config
    
    # Create Fernet cipher from key
    # NestJS uses a 32-byte key derived from ENCRYPTION_KEY
    key_bytes = encryption_key.encode()
    if len(key_bytes) < 32:
        key_bytes = key_bytes + b'\x00' * (32 - len(key_bytes))
    elif len(key_bytes) > 32:
        key_bytes = key_bytes[:32]
    
    fernet_key = base64.urlsafe_b64encode(key_bytes)
    cipher = Fernet(fernet_key)
    
    decrypted = dict(config)
    
    def try_decrypt(value):
        if not isinstance(value, str):
            return value
        try:
            return cipher.decrypt(value.encode()).decode()
        except:
            return value
    
    # Decrypt based on connection type
    if connection_type in ['postgres', 'postgresql', 'mysql']:
        if 'password' in decrypted:
            decrypted['password'] = try_decrypt(decrypted['password'])
        if decrypted.get('ssl', {}).get('ca_cert'):
            decrypted['ssl']['ca_cert'] = try_decrypt(decrypted['ssl']['ca_cert'])
    elif connection_type == 'mongodb':
        if 'connection_string' in decrypted:
            decrypted['connection_string'] = try_decrypt(decrypted['connection_string'])
        if 'password' in decrypted:
            decrypted['password'] = try_decrypt(decrypted['password'])
    
    return decrypted


@app.post("/organizations/{organization_id}/pipelines/{pipeline_id}/pause")
async def pause_pipeline(
    organization_id: str,
    pipeline_id: str,
    token: Optional[str] = Depends(verify_token),
):
    """
    Pause a running pipeline.
    """
    import psycopg2
    
    try:
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise HTTPException(status_code=500, detail="DATABASE_URL not configured")
        
        database_url = database_url.strip('"').strip("'")
        conn = psycopg2.connect(database_url)
        cursor = conn.cursor()
        
        try:
            # Update pipeline status
            cursor.execute("""
                UPDATE pipelines 
                SET status = 'paused', updated_at = NOW()
                WHERE id = %s AND organization_id = %s AND deleted_at IS NULL
                RETURNING id
            """, (pipeline_id, organization_id))
            
            result = cursor.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail=f"Pipeline {pipeline_id} not found")
            
            conn.commit()
            
            return {
                "success": True,
                "pipelineId": pipeline_id,
                "status": "paused",
            }
        finally:
            cursor.close()
            conn.close()
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Pause pipeline error: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Failed to pause pipeline: {str(e)}")


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8001))
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        reload=True,
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
    )
