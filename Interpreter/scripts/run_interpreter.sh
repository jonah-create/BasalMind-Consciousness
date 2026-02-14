#!/bin/bash
# Interpreter Runner Script

set -e  # Exit on error

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
INTERPRETER_DIR="$(dirname "$SCRIPT_DIR")"

echo "🚀 Starting BasalMind Interpreter..."
echo "   Directory: $INTERPRETER_DIR"

# Load environment
if [ -f "$INTERPRETER_DIR/.env" ]; then
    export $(grep -v '^#' "$INTERPRETER_DIR/.env" | xargs)
    echo "✅ Loaded .env configuration"
else
    echo "⚠️ No .env file found"
fi

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "   Python: $PYTHON_VERSION"

# Check if venv exists
if [ ! -d "$INTERPRETER_DIR/venv" ]; then
    echo "⚠️ Virtual environment not found. Run scripts/setup_interpreter.sh first"
    exit 1
fi

# Activate virtual environment
source "$INTERPRETER_DIR/venv/bin/activate"

# Check database connections
echo ""
echo "🔍 Checking database connections..."

# Test TimescaleDB
if docker exec basalmind-timescaledb psql -U basalmind -d basalmind_events -c "SELECT 1" > /dev/null 2>&1; then
    echo "   ✅ TimescaleDB (basalmind_events)"
else
    echo "   ❌ TimescaleDB connection failed"
    exit 1
fi

# Test PostgreSQL
if docker exec basalmind-timescaledb psql -U basalmind -d basalmind_interpreter_postgresql -c "SELECT 1" > /dev/null 2>&1; then
    echo "   ✅ PostgreSQL (basalmind_interpreter_postgresql)"
else
    echo "   ❌ PostgreSQL connection failed"
    exit 1
fi

# Test Neo4j
if echo "RETURN 1" | docker exec -i shadowcaster-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" > /dev/null 2>&1; then
    echo "   ✅ Neo4j"
else
    echo "   ❌ Neo4j connection failed"
    exit 1
fi

echo ""
echo "🎯 Starting Interpreter engine..."
echo "   Batch window: ${BATCH_WINDOW_SECONDS:-30}s"
echo "   Batch size: ${BATCH_SIZE:-1000} events"
echo "   Processing interval: ${PROCESSING_INTERVAL_SECONDS:-30}s"
echo ""

# Run the engine
cd "$INTERPRETER_DIR"
python3 -m interpreter.engine

# Deactivate venv on exit
deactivate
