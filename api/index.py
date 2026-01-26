"""
Vercel serverless function entrypoint for FastAPI ETL service.

This file wraps the FastAPI app from main.py using Mangum,
which provides an ASGI-to-AWS Lambda/API Gateway adapter.
Vercel's Python runtime uses a similar interface.
"""

import os
import sys
from pathlib import Path

# Add the parent directory to Python path so we can import main
# This is needed because Vercel runs from api/ directory
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.insert(0, str(parent_dir))

# Set working directory to parent (where main.py, connectors/, etc. are)
os.chdir(parent_dir)

# Import the FastAPI app from main.py
from main import app

# For Vercel, we need to export a handler function
# Mangum wraps FastAPI ASGI app for serverless environments
try:
    from mangum import Mangum
    
    # Create Mangum handler wrapping the FastAPI app
    handler = Mangum(app, lifespan="off")
    
except ImportError:
    # Fallback: If mangum is not available, raise error with helpful message
    raise ImportError(
        "Mangum is required for Vercel deployment. "
        "Install it with: pip install mangum==0.17.0"
    )

# Export handler for Vercel
__all__ = ["handler"]
