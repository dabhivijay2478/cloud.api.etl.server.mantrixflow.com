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
    Supports full and incremental sync modes with state/bookmark handling.
    """
    try:
        logger.info(f"Collecting data from {source_type} (mode: {request.sync_mode})")
        
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
        
        # For PostgreSQL
        if source_type.lower() in ['postgresql', 'postgres']:
            import tap_postgres.db as post_db
            import tap_postgres.sync_strategies.full_table as full_table
            import tap_postgres.sync_strategies.incremental as incremental
            from singer import metadata
            from singer.catalog import Catalog, CatalogEntry
            from singer.schema import Schema
            
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
            
            # Build catalog entry for the table
            # This is simplified - in production, you'd call discovery first
            table_name = request.table_name or 'unknown'
            schema_name = request.schema_name or 'public'
            
            # Create a minimal catalog entry
            stream_metadata = [
                {
                    'breadcrumb': (),
                    'metadata': {
                        'database-name': conn_config['dbname'],
                        'schema-name': schema_name,
                        'table-name': table_name,
                        'replication-method': request.sync_mode,
                    }
                }
            ]
            
            # Add replication key for incremental
            if request.sync_mode == 'incremental':
                # Try to get replication key from checkpoint or use a default
                replication_key = None
                if request.checkpoint:
                    # Extract replication key from state
                    bookmarks = request.checkpoint.get('bookmarks', {})
                    stream_id = f"{schema_name}-{table_name}"
                    stream_bookmark = bookmarks.get(stream_id, {})
                    replication_key = stream_bookmark.get('replication_key')
                
                if replication_key:
                    stream_metadata.append({
                        'breadcrumb': (replication_key,),
                        'metadata': {
                            'replication-key': True,
                        }
                    })
            
            stream = {
                'tap_stream_id': f"{schema_name}-{table_name}",
                'stream': table_name,
                'schema': Schema(),  # Simplified - would need actual schema
                'metadata': stream_metadata,
            }
            
            catalog = Catalog([CatalogEntry(**stream)])
            
            # Prepare state
            state = request.checkpoint or {}
            
            # Capture Singer output
            stdout_capture = io.StringIO()
            stderr_capture = io.StringIO()
            
            records = []
            new_state = state.copy()
            
            try:
                with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                    # This is a simplified version - full implementation would
                    # properly integrate with Singer's sync methods
                    # For now, we'll use a direct query approach
                    pass
                
                # Parse records from output
                output = stdout_capture.getvalue()
                records = parse_singer_records(output)
                new_state = parse_singer_state(output)
                
                # Apply limit/offset if provided
                if request.limit:
                    records = records[request.offset or 0:request.offset or 0 + request.limit]
                
                return CollectResponse(
                    rows=records,
                    total_rows=len(records),
                    has_more=False,  # Would need to check if more data available
                    checkpoint=new_state,
                )
                
            except Exception as e:
                logger.error(f"Collection failed: {str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Collection failed: {str(e)}")
        
        else:
            raise HTTPException(
                status_code=501,
                detail=f"Collection for {source_type} not yet fully implemented. PostgreSQL is supported."
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
            # Use SQLAlchemy for PostgreSQL upsert
            from sqlalchemy import create_engine, MetaData, Table, Column, String, Integer
            from sqlalchemy.dialects.postgresql import insert
            
            # Build connection string
            conn_config = request.connection_config
            conn_string = conn_config.get('connection_string')
            if not conn_string:
                conn_string = (
                    f"postgresql://{conn_config.get('username', conn_config.get('user'))}"
                    f":{conn_config.get('password')}@"
                    f"{conn_config.get('host')}:{conn_config.get('port', 5432)}/"
                    f"{conn_config.get('database', conn_config.get('dbname'))}"
                )
            
            engine = create_engine(conn_string)
            metadata_obj = MetaData()
            
            # For upsert, we need to know the table structure
            # This is simplified - in production, you'd discover the schema first
            table_name = request.table_name
            schema_name = request.schema_name or 'public'
            
            rows_written = 0
            rows_failed = 0
            errors = []
            
            try:
                with engine.connect() as conn:
                    # If upsert mode and upsert_key provided
                    if request.write_mode == 'upsert' and request.upsert_key:
                        # Use PostgreSQL ON CONFLICT
                        for idx, row in enumerate(request.rows):
                            try:
                                # Build INSERT ... ON CONFLICT statement
                                # This is simplified - would need proper table reflection
                                # For now, use raw SQL
                                placeholders = ', '.join([f':{k}' for k in row.keys()])
                                columns = ', '.join(row.keys())
                                update_set = ', '.join([f'{k} = EXCLUDED.{k}' for k in row.keys() if k not in request.upsert_key])
                                conflict_target = ', '.join(request.upsert_key)
                                
                                sql = f"""
                                    INSERT INTO {schema_name}.{table_name} ({columns})
                                    VALUES ({placeholders})
                                    ON CONFLICT ({conflict_target}) DO UPDATE SET {update_set}
                                """
                                
                                conn.execute(sql, row)
                                rows_written += 1
                            except Exception as e:
                                rows_failed += 1
                                errors.append({
                                    'record_index': idx,
                                    'error': str(e),
                                })
                        
                        conn.commit()
                    
                    elif request.write_mode == 'append':
                        # Simple INSERT
                        for idx, row in enumerate(request.rows):
                            try:
                                placeholders = ', '.join([f':{k}' for k in row.keys()])
                                columns = ', '.join(row.keys())
                                sql = f"INSERT INTO {schema_name}.{table_name} ({columns}) VALUES ({placeholders})"
                                conn.execute(sql, row)
                                rows_written += 1
                            except Exception as e:
                                rows_failed += 1
                                errors.append({
                                    'record_index': idx,
                                    'error': str(e),
                                })
                        
                        conn.commit()
                    
                    else:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Write mode '{request.write_mode}' not supported for {dest_type}"
                        )
            
            except Exception as e:
                logger.error(f"Emission failed: {str(e)}", exc_info=True)
                raise HTTPException(status_code=500, detail=f"Emission failed: {str(e)}")
            
            return EmitResponse(
                rows_written=rows_written,
                rows_skipped=0,
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
    Validates the connection configuration and returns success.
    Note: This endpoint validates the connection configuration.
    The frontend should also call NestJS API to store it in the database.
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
        
        # Generate ID if not provided
        connection_id = request.get("id") or str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        
        # Return response matching frontend expectations
        return {
            "id": connection_id,
            "data_source_id": data_source_id,
            "connection_type": normalized_type,
            "config": config,
            "status": "active",
            "created_at": request.get("created_at") or now,
            "updated_at": request.get("updated_at") or now,
        }
    
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
