#!/bin/bash
# Quick start script for integration testing

set -e

echo "🚀 olostep-spark Integration Test Setup"
echo "======================================"

# Check for API key
if [ -z "$OLOSTEP_API_KEY" ]; then
    if [ -f ".env" ]; then
        echo "📝 Loading .env file..."
        export $(cat .env | xargs)
    else
        echo "❌ Error: OLOSTEP_API_KEY not set"
        echo ""
        echo "Please set your API key:"
        echo "  export OLOSTEP_API_KEY='your-api-key-here'"
        echo ""
        echo "Or create a .env file:"
        echo "  cp .env.example .env"
        echo "  # Edit .env and add your API key"
        exit 1
    fi
fi

echo "✅ API key found: ${OLOSTEP_API_KEY:0:8}..."

# Check for venv
if [ ! -d "venv" ]; then
    echo "📦 Creating virtual environment..."
    python3.11 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip setuptools wheel
    pip install -e ".[dev]"
else
    echo "📦 Virtual environment exists"
    source venv/bin/activate
fi

# Check Java
echo "☕ Checking Java version..."
java_version=$(java -version 2>&1 | grep -oP 'version "\K[^"]*' | cut -d. -f1)
if [ "$java_version" -lt 17 ]; then
    echo "⚠️  Java version is too old (requires 17+). Tests may fail."
fi

echo ""
echo "✨ Running integration tests..."
echo ""

pytest tests/test_integration.py -m integration -v --tb=short

echo ""
echo "✅ Integration tests complete!"
