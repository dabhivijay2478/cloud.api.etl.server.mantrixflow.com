#!/bin/bash
# Build script for Vercel deployment
# Installs Python dependencies and Singer taps

set -e

echo "🔨 Building Python ETL service for Vercel..."

# Install main dependencies
echo "📦 Installing Python dependencies..."
pip install -r requirements.txt

# Install Singer taps (local connectors)
echo "🔌 Installing Singer taps..."
if [ -d "connectors/tap-postgres" ]; then
    echo "  - Installing tap-postgres..."
    pip install -e connectors/tap-postgres
fi

if [ -d "connectors/tap-mysql" ]; then
    echo "  - Installing tap-mysql..."
    pip install -e connectors/tap-mysql
fi

if [ -d "connectors/tap-mongodb" ]; then
    echo "  - Installing tap-mongodb..."
    pip install -e connectors/tap-mongodb
fi

echo "✅ Build complete!"
