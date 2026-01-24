"""
Utility functions for ETL operations
- State/bookmark handling for incremental syncs
- Streaming helpers
- Singer tap integration helpers
"""

import json
import logging
from typing import Any, Dict, Optional, List
import io
import sys

logger = logging.getLogger(__name__)


def parse_singer_state(state_output: str) -> Dict[str, Any]:
    """
    Parse Singer state message from tap output.
    
    Singer taps output state messages like:
    {"type": "STATE", "value": {"bookmarks": {...}}}
    """
    state = {}
    
    if not state_output:
        return state
    
    for line in state_output.strip().split('\n'):
        if not line.strip():
            continue
        
        try:
            message = json.loads(line)
            if message.get('type') == 'STATE':
                state = message.get('value', {})
        except json.JSONDecodeError:
            continue
    
    return state


def extract_bookmark_from_state(
    state: Dict[str, Any],
    stream_name: str,
    bookmark_key: str = 'replication_key_value',
) -> Optional[Any]:
    """
    Extract bookmark value from Singer state for a specific stream.
    
    Args:
        state: Singer state dict
        stream_name: Name of the stream/table
        bookmark_key: Key to extract (default: 'replication_key_value')
        
    Returns:
        Bookmark value or None
    """
    if not state:
        return None
    
    bookmarks = state.get('bookmarks', {})
    stream_bookmark = bookmarks.get(stream_name, {})
    
    return stream_bookmark.get(bookmark_key)


def merge_state(
    current_state: Dict[str, Any],
    new_state: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Merge new state into current state, preserving all streams.
    """
    if not current_state:
        return new_state
    
    if not new_state:
        return current_state
    
    # Merge bookmarks
    current_bookmarks = current_state.get('bookmarks', {})
    new_bookmarks = new_state.get('bookmarks', {})
    
    merged_bookmarks = {**current_bookmarks, **new_bookmarks}
    
    return {
        **current_state,
        'bookmarks': merged_bookmarks,
    }


def build_singer_config(
    connection_config: Dict[str, Any],
    source_config: Dict[str, Any],
    table_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    query: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Build Singer tap configuration from connection and source configs.
    
    Maps common connection fields to Singer tap-specific configs.
    """
    config = {}
    
    # Connection config mapping
    if 'host' in connection_config:
        config['host'] = connection_config['host']
    if 'port' in connection_config:
        config['port'] = connection_config['port']
    if 'database' in connection_config:
        config['dbname'] = connection_config['database']
    elif 'dbname' in connection_config:
        config['dbname'] = connection_config['dbname']
    if 'username' in connection_config:
        config['user'] = connection_config['username']
    elif 'user' in connection_config:
        config['user'] = connection_config['user']
    if 'password' in connection_config:
        config['password'] = connection_config['password']
    
    # Connection string (for MongoDB, PostgreSQL, etc.)
    if 'connection_string' in connection_config:
        config['connection_string'] = connection_config['connection_string']
    if 'connection_string_mongo' in connection_config:
        config['connection_string_mongo'] = connection_config['connection_string_mongo']
    
    # SSL/TLS config
    if 'ssl' in connection_config:
        config['ssl'] = connection_config['ssl']
    if 'ssl_mode' in connection_config:
        config['ssl_mode'] = connection_config['ssl_mode']
    
    # MongoDB-specific
    if 'auth_source' in connection_config:
        config['auth_source'] = connection_config['auth_source']
    if 'replica_set' in connection_config:
        config['replica_set'] = connection_config['replica_set']
    if 'tls' in connection_config:
        config['tls'] = connection_config['tls']
    
    # Source config (table/schema/query)
    if table_name:
        config['table'] = table_name
    if schema_name:
        config['schema'] = schema_name
    if query:
        config['query'] = query
    
    # Merge any additional source_config
    config.update(source_config)
    
    return config


def capture_stdout_stderr(func, *args, **kwargs):
    """
    Capture stdout and stderr from a function execution.
    Returns (result, stdout, stderr)
    """
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    
    stdout_capture = io.StringIO()
    stderr_capture = io.StringIO()
    
    sys.stdout = stdout_capture
    sys.stderr = stderr_capture
    
    try:
        result = func(*args, **kwargs)
    finally:
        sys.stdout = old_stdout
        sys.stderr = old_stderr
    
    stdout_output = stdout_capture.getvalue()
    stderr_output = stderr_capture.getvalue()
    
    return result, stdout_output, stderr_output


def parse_singer_records(output: str) -> List[Dict[str, Any]]:
    """
    Parse Singer RECORD messages from tap output.
    
    Singer taps output record messages like:
    {"type": "RECORD", "stream": "table_name", "record": {...}}
    """
    records = []
    
    if not output:
        return records
    
    for line in output.strip().split('\n'):
        if not line.strip():
            continue
        
        try:
            message = json.loads(line)
            if message.get('type') == 'RECORD':
                record = message.get('record', {})
                stream = message.get('stream', '')
                # Add stream name to record for reference
                record['_stream'] = stream
                records.append(record)
        except json.JSONDecodeError:
            continue
    
    return records


def normalize_source_type(source_type: str) -> str:
    """
    Normalize source type string to match connector names.
    """
    normalized = source_type.lower().strip()
    
    # Map common variations
    if normalized in ['postgres', 'postgresql']:
        return 'postgresql'
    if normalized in ['mysql', 'mariadb']:
        return 'mysql'
    if normalized in ['mongodb', 'mongo']:
        return 'mongodb'
    
    return normalized
