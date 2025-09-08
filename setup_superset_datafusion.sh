#!/bin/bash

# Setup Superset with DataFusion integration

echo "Setting up Superset with DataFusion integration..."

# Activate virtual environment
source .venv/bin/activate

# Install Superset
echo "Installing Superset..."
pip install apache-superset==4.0.1

# Install additional dependencies
echo "Installing additional dependencies..."
pip install pandas numpy pyarrow fastparquet pymysql

# Set environment variables
export FLASK_APP=superset.app:create_app
export SUPERSET_CONFIG_PATH=superset_config_datafusion.py

# Initialize Superset database
echo "Initializing Superset database..."
superset db upgrade

# Create admin user
echo "Creating admin user..."
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@example.com \
    --password admin

# Initialize Superset
echo "Initializing Superset..."
superset init

echo "Superset setup completed!"
echo "You can now start Superset with:"
echo "  superset run -h 0.0.0.0 -p 8088"
echo ""
echo "To add DataFusion as a database:"
echo "1. Go to Settings > Database Connections"
echo "2. Click 'Connect a database'"
echo "3. Select 'Other' as database type"
echo "4. Use 'datafusion://' as the SQLAlchemy URI"
echo "5. Click 'Test Connection' and then 'Connect'"
