#!/bin/bash
# BasalMind Observer Startup Script
# Created: 2026-02-11
# Purpose: Start Observer service with proper environment

set -e

OBSERVER_DIR="/opt/basalmind/BasalMind_Consciousness/Observer"
LOG_FILE="/tmp/observer.log"
PID_FILE="/tmp/observer.pid"

cd $OBSERVER_DIR

# Check if already running
if [ -f $PID_FILE ]; then
    PID=$(cat $PID_FILE)
    if ps -p $PID > /dev/null 2>&1; then
        echo "❌ Observer already running (PID: $PID)"
        echo "   Use: kill $PID to stop it first"
        exit 1
    else
        echo "⚠️  Removing stale PID file"
        rm $PID_FILE
    fi
fi

# Check dependencies
echo "🔍 Checking dependencies..."

# Redis
if ! redis-cli -p 6390 -a '_t9iV2e(p9voC34HpkvxirRoy%8cSYcf' PING > /dev/null 2>&1; then
    echo "❌ Redis not available on port 6390"
    exit 1
fi
echo "✅ Redis connected"

# NATS
if ! curl -s http://localhost:8222/varz > /dev/null 2>&1; then
    echo "❌ NATS not available on port 4222"
    echo "   Start with: nats-server --jetstream"
    exit 1
fi
echo "✅ NATS connected"

# Neo4j
if ! docker ps | grep -q neo4j; then
    echo "⚠️  Neo4j container not running (non-fatal)"
else
    echo "✅ Neo4j running"
fi

# TimescaleDB
if ! pg_isready -h localhost -p 5432 -U postgres > /dev/null 2>&1; then
    echo "❌ TimescaleDB not available on port 5432"
    exit 1
fi
echo "✅ TimescaleDB connected"

# Start Observer
echo ""
echo "🚀 Starting Observer on port 5002..."
nohup python3 -m uvicorn observer.main:app --host 0.0.0.0 --port 5002 > $LOG_FILE 2>&1 &
OBSERVER_PID=$!

echo $OBSERVER_PID > $PID_FILE

# Wait for startup
sleep 3

# Verify it's running
if ps -p $OBSERVER_PID > /dev/null 2>&1; then
    echo "✅ Observer started successfully"
    echo "   PID: $OBSERVER_PID"
    echo "   Port: 5002"
    echo "   Logs: $LOG_FILE"
    echo ""
    echo "📊 Test endpoints:"
    echo "   curl -X POST http://localhost:5002/observe/internal/event -H 'Content-Type: application/json' -d '{"event_type":"test","payload":{}}' "
    echo ""
    echo "📋 View logs: tail -f $LOG_FILE"
    echo "🛑 Stop Observer: kill $OBSERVER_PID"
else
    echo "❌ Observer failed to start. Check logs: $LOG_FILE"
    rm $PID_FILE
    exit 1
fi
