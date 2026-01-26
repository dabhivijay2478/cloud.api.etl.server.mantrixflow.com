"""
Vercel serverless function entrypoint for FastAPI ETL service.

For @vercel/python builder, export the FastAPI app directly.
Vercel's builder should handle FastAPI/ASGI apps automatically.
"""

import os
import sys
from pathlib import Path

# Add the parent directory to Python path
current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.insert(0, str(parent_dir))

# Set working directory to parent
os.chdir(parent_dir)

# Import and export the FastAPI app
from main import app

# Export app for Vercel - the @vercel/python builder should detect FastAPI
# If this doesn't work, we may need to use Mangum wrapper
