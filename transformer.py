"""
Transformer Module
Safely executes user-provided Python transform scripts on records.

The transform script must define a function:
    def transform(record):
        # Use record.get("source_field") to read from source
        # Return dict with destination keys
        return {"destination_field": record.get("source_field")}
"""

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Restricted globals for safe execution
RESTRICTED_GLOBALS = {
    '__builtins__': {
        # Safe builtins only
        'abs': abs,
        'all': all,
        'any': any,
        'bool': bool,
        'dict': dict,
        'float': float,
        'int': int,
        'len': len,
        'list': list,
        'max': max,
        'min': min,
        'range': range,
        'round': round,
        'str': str,
        'sum': sum,
        'tuple': tuple,
        'type': type,
        'zip': zip,
        # String methods
        'str': str,
        # Type checking
        'isinstance': isinstance,
        'hasattr': hasattr,
        'getattr': getattr,
        'setattr': setattr,
        # Exceptions
        'ValueError': ValueError,
        'TypeError': TypeError,
        'KeyError': KeyError,
        'AttributeError': AttributeError,
        # None
        'None': None,
        'True': True,
        'False': False,
    },
    'json': json,
}


def safe_exec_transform(
    records: List[Dict[str, Any]],
    transform_script: str,
) -> Dict[str, Any]:
    """
    Safely execute a user-provided transform script on each record.
    
    Args:
        records: List of source records to transform
        transform_script: Python code
            that defines a `transform(record)` function
            
    Returns:
        Dict with:
            - transformed_rows: List of transformed records
            - errors: List of error dicts for failed transformations
            
    The transform script should:
        - Define a function: def transform(record):
        - Use record.get("source_field") to read from source
        - Return a dict with destination keys
    """
    if not transform_script or not transform_script.strip():
        logger.warning("Empty transform script provided, returning records as-is")
        return {
            'transformed_rows': records,
            'errors': [],
        }
    
    # Prepare the execution environment
    exec_globals = RESTRICTED_GLOBALS.copy()
    exec_locals = {}
    
    try:
        # Execute the transform script to define the transform function
        exec(transform_script, exec_globals, exec_locals)
    except Exception as e:
        error_msg = f"Failed to compile transform script: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            'transformed_rows': [],
            'errors': [{
                'record_index': -1,
                'error': error_msg,
                'type': 'compilation_error',
            }],
        }
    
    # Get the transform function
    transform_func = exec_locals.get('transform')
    if not transform_func or not callable(transform_func):
        error_msg = "Transform script must define a function named 'transform(record)'"
        logger.error(error_msg)
        return {
            'transformed_rows': [],
            'errors': [{
                'record_index': -1,
                'error': error_msg,
                'type': 'missing_transform_function',
            }],
        }
    
    # Transform each record
    transformed_rows = []
    errors = []
    
    for idx, record in enumerate(records):
        try:
            # Prepare locals for each record execution
            record_locals = {
                'record': record,
                'json': json,
            }
            
            # Execute transform function
            transformed_record = transform_func(record)
            
            # Validate result
            if not isinstance(transformed_record, dict):
                raise TypeError(f"Transform function must return a dict, got {type(transformed_record)}")
            
            transformed_rows.append(transformed_record)
            
        except Exception as e:
            error_msg = f"Transform failed for record {idx}: {str(e)}"
            logger.warning(error_msg, exc_info=True)
            errors.append({
                'record_index': idx,
                'error': error_msg,
                'type': type(e).__name__,
                'record_preview': str(record)[:200] if record else None,
            })
    
    logger.info(f"Transformed {len(transformed_rows)}/{len(records)} records successfully")
    
    return {
        'transformed_rows': transformed_rows,
        'errors': errors,
    }


def validate_transform_script(transform_script: str) -> Dict[str, Any]:
    """
    Validate a transform script without executing it on data.
    
    Returns:
        Dict with 'valid' (bool) and 'error' (str, optional)
    """
    if not transform_script or not transform_script.strip():
        return {
            'valid': False,
            'error': 'Transform script is empty',
        }
    
    exec_globals = RESTRICTED_GLOBALS.copy()
    exec_locals = {}
    
    try:
        exec(transform_script, exec_globals, exec_locals)
    except Exception as e:
        return {
            'valid': False,
            'error': f"Compilation error: {str(e)}",
        }
    
    transform_func = exec_locals.get('transform')
    if not transform_func or not callable(transform_func):
        return {
            'valid': False,
            'error': "Transform script must define a function named 'transform(record)'",
        }
    
    # Test with a sample record
    try:
        test_record = {'test_field': 'test_value'}
        result = transform_func(test_record)
        if not isinstance(result, dict):
            return {
                'valid': False,
                'error': f"Transform function must return a dict, got {type(result)}",
            }
    except Exception as e:
        return {
            'valid': False,
            'error': f"Transform function test failed: {str(e)}",
        }
    
    return {
        'valid': True,
    }
