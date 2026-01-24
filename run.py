#!/usr/bin/env python3
"""
Python ETL Service Runner
Alternative to run.sh - can be used on Windows or when shell scripts aren't available
"""

import os
import sys
import subprocess
from pathlib import Path

def check_python_version():
    """Check if Python version is 3.9+"""
    if sys.version_info < (3, 9):
        print("❌ Python 3.9 or higher is required")
        print(f"   Current version: {sys.version}")
        sys.exit(1)
    print(f"✓ Python version: {sys.version.split()[0]}")

def check_and_install_dependencies():
    """Check and install dependencies if needed"""
    try:
        import fastapi
        print("✓ FastAPI installed")
    except ImportError:
        print("📦 Installing Python dependencies...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pip"])
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])

def check_and_install_taps():
    """Check and install Singer taps if needed"""
    base_dir = Path(__file__).parent
    
    taps = [
        ("tap-postgres", "tap_postgres"),
        ("tap-mysql", "tap_mysql"),
        ("tap-mongodb", "tap_mongodb"),
    ]
    
    for tap_dir, import_name in taps:
        tap_path = base_dir / "connectors" / tap_dir
        if tap_path.exists():
            try:
                __import__(import_name)
                print(f"✓ {tap_dir} installed")
            except ImportError:
                print(f"📦 Installing {tap_dir}...")
                subprocess.check_call([
                    sys.executable, "-m", "pip", "install", "-e", str(tap_path)
                ])

def load_env():
    """Load environment variables from .env file"""
    env_file = Path(__file__).parent / ".env"
    if env_file.exists():
        print("✓ Loading environment variables from .env")
        from dotenv import load_dotenv
        load_dotenv(env_file)
    else:
        print("⚠ .env file not found. Using defaults.")

def check_port():
    """Check if port is available"""
    import socket
    port = int(os.getenv("PORT", "8001"))
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("localhost", port))
        sock.close()
        return port
    except OSError:
        print(f"❌ Port {port} is already in use.")
        print(f"   Please stop the service using that port or change PORT in .env")
        sys.exit(1)

def main():
    """Main runner function"""
    print("🚀 Starting Python ETL Service...\n")
    
    # Change to script directory
    os.chdir(Path(__file__).parent)
    
    # Check Python version
    check_python_version()
    
    # Load environment
    load_env()
    
    # Check and install dependencies
    check_and_install_dependencies()
    
    # Check and install taps
    check_and_install_taps()
    
    # Check port
    port = check_port()
    
    # Configuration
    log_level = os.getenv("LOG_LEVEL", "INFO")
    
    print(f"\n✓ Configuration:")
    print(f"  - Port: {port}")
    print(f"  - Log Level: {log_level}")
    print()
    print(f"🚀 Starting FastAPI server on port {port}...")
    print(f"   API will be available at: http://localhost:{port}")
    print(f"   API docs: http://localhost:{port}/docs")
    print()
    print("Press Ctrl+C to stop the server\n")
    
    # Start the service
    try:
        import uvicorn
        uvicorn.run(
            "main:app",
            host="0.0.0.0",
            port=port,
            reload=True,
            log_level=log_level.lower(),
        )
    except KeyboardInterrupt:
        print("\n\n👋 Server stopped")
    except Exception as e:
        print(f"\n❌ Error starting server: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
