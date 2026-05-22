#!/bin/bash
# Run tests with correct Python environment

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_PYTHON="$SCRIPT_DIR/venv/bin/python"

# Verify venv exists
if [ ! -f "$VENV_PYTHON" ]; then
    echo "❌ Error: Virtual environment not found at $VENV_PYTHON"
    echo "Create it with: python3.11 -m venv venv && source venv/bin/activate && pip install -e \".[dev]\""
    exit 1
fi

# Set Python paths for PySpark
export PYSPARK_PYTHON="$VENV_PYTHON"
export PYSPARK_DRIVER_PYTHON="$VENV_PYTHON"

# Activate venv
source "$SCRIPT_DIR/venv/bin/activate"

# Run pytest with all arguments passed through
pytest "$@"
