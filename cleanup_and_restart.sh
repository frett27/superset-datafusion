#!/bin/bash

# DataFusion + Superset Cleanup and Restart Script
# This script handles all the issues we've encountered and provides a clean restart

set -e  # Exit on any error

echo "🧹 DataFusion + Superset Cleanup and Restart Script"
echo "=================================================="

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Step 1: Stop Superset and clean up processes
print_status "Step 1: Stopping Superset and cleaning up processes..."
pkill -f "superset run" || true
pkill -f "superset" || true
sleep 2
print_success "Processes stopped"

# Step 2: Stop and clean up Docker containers
print_status "Step 2: Stopping and cleaning up Docker containers..."
docker compose down || true
docker volume rm datafusion-sqlalchemy_mysql_data || true
print_success "Docker containers and volumes cleaned up"

# Step 3: Activate virtual environment
print_status "Step 3: Activating virtual environment..."
source .venv/bin/activate
print_success "Virtual environment activated"

# Step 4: Rebuild and reinstall DataFusion packages
print_status "Step 4: Rebuilding and reinstalling DataFusion packages..."

# Rebuild the Rust package
print_status "Rebuilding Rust package..."
cargo build --release
print_success "Rust package rebuilt"

# Reinstall datafusion_dbapi
print_status "Reinstalling datafusion_dbapi..."
pip install -e . --force-reinstall
print_success "datafusion_dbapi reinstalled"

# Reinstall sqlalchemy_datafusion
print_status "Reinstalling sqlalchemy_datafusion..."
cd python/sqlalchemy_datafusion_pkg
pip install -e . --force-reinstall
cd ../..
print_success "sqlalchemy_datafusion reinstalled"

# Step 5: Start MySQL
print_status "Step 5: Starting MySQL..."
docker compose up -d mysql
print_status "Waiting for MySQL to be ready..."
sleep 15
print_success "MySQL started"

# Step 6: Set environment variables
print_status "Step 6: Setting environment variables..."
export FLASK_APP=superset.app:create_app
export SUPERSET_CONFIG_PATH=superset_config_datafusion.py
print_success "Environment variables set"

# Step 7: Initialize Superset database
print_status "Step 7: Initializing Superset database..."
superset db upgrade
print_success "Database upgraded"

# Step 8: Initialize Superset
print_status "Step 8: Initializing Superset..."
superset init
print_success "Superset initialized"

# Step 9: Create admin user
print_status "Step 9: Creating admin user..."
superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin
print_success "Admin user created"

# Step 10: Test the integration
print_status "Step 10: Testing the integration..."
python3 test_superset_integration.py
print_success "Integration test passed"

# Step 11: Start Superset
print_status "Step 11: Starting Superset..."
print_warning "Starting Superset in background. Check the logs for any issues."
nohup superset run -h 0.0.0.0 -p 8088 --with-threads --reload --debugger > superset.log 2>&1 &
SUPERSET_PID=$!
echo $SUPERSET_PID > superset.pid

# Wait for Superset to start
print_status "Waiting for Superset to start..."
sleep 20

# Test if Superset is running
if curl -s "http://localhost:8088/health" > /dev/null; then
    print_success "Superset is running successfully!"
    print_success "You can access it at: http://localhost:8088"
    print_success "Login with: admin/admin"
    print_success "Superset PID: $SUPERSET_PID"
    print_success "Logs: tail -f superset.log"
else
    print_error "Superset failed to start. Check the logs:"
    print_error "tail -f superset.log"
    exit 1
fi

echo ""
echo "🎉 Cleanup and restart completed successfully!"
echo "=============================================="
echo ""
echo "📋 Next steps:"
echo "1. Go to http://localhost:8088"
echo "2. Login with admin/admin"
echo "3. Go to SQL Lab"
echo "4. Select the DataFusion database"
echo "5. Check if tables appear in the schema browser"
echo ""
echo "🔧 Useful commands:"
echo "- View logs: tail -f superset.log"
echo "- Stop Superset: kill \$(cat superset.pid)"
echo "- Restart: ./cleanup_and_restart.sh"
echo ""
echo "📁 Files created:"
echo "- superset.log: Superset application logs"
echo "- superset.pid: Superset process ID"
echo ""
