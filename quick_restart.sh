#!/bin/bash

# Quick Superset Restart Script
# Use this when you just need to restart Superset without full cleanup

set -e

echo "🚀 Quick Superset Restart"
echo "========================"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

# Stop Superset
print_status "Stopping Superset..."
pkill -f "superset run" || true
sleep 2

# Activate virtual environment
source .venv/bin/activate

# Set environment variables
export FLASK_APP=superset.app:create_app
export SUPERSET_CONFIG_PATH=superset_config_datafusion.py

# Start Superset
print_status "Starting Superset..."
nohup superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger > superset.log 2>&1 &
SUPERSET_PID=$!
echo $SUPERSET_PID > superset.pid

# Wait and test
sleep 15
if curl -s "http://localhost:8088/health" > /dev/null; then
    print_success "Superset restarted successfully!"
    print_success "Access: http://localhost:8088"
    print_success "PID: $SUPERSET_PID"
else
    echo "❌ Superset failed to start. Check logs: tail -f superset.log"
    exit 1
fi
