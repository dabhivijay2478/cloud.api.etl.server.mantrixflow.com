"""
Alternative handler using Mangum if direct FastAPI export doesn't work.
Rename this to index.py if the direct app export fails.
"""

import os
import sys
from pathlib import Path

current_dir = Path(__file__).parent
parent_dir = current_dir.parent
sys.path.insert(0, str(parent_dir))
os.chdir(parent_dir)

from main import app
from mangum import Mangum

# Create handler using Mangum
handler = Mangum(app, lifespan="off")
