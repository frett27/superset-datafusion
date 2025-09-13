#!/bin/bash

# Setup Superset with DataFusion integration
# you must have a mysql database running on the localhost:3306
# with the username "superset" and the password "superset_password" (for tests purposes)

echo "Setting up Superset with DataFusion integration..."

# Activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install Superset from source
echo "Installing Superset..."
cd superset
git checkout -f 4.0.1

make

cd superset-frontend
nvm use 23
npm ci
npm build
cd ..
cd ..

# install from source in venv
pip install -e .
pip install -e superset
pip install -e python/superset_datafusion
pip install -e python/sqlalchemy_datafusion_pkg


# install from binary
## pip install apache-superset==4.0.1

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


