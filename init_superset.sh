#!/bin/bash
set -e

echo "🚀 Initializing Superset with DataFusion support..."

# Wait for Superset to be ready
echo "⏳ Waiting for Superset to be ready..."
max_attempts=30
attempt=0
while [ $attempt -lt $max_attempts ]; do
  if curl -f http://localhost:8088/health >/dev/null 2>&1; then
    echo "✅ Superset is ready!"
    break
  fi
  echo "Attempt $((attempt + 1))/$max_attempts: Waiting for Superset..."
  sleep 10
  attempt=$((attempt + 1))
done

if [ $attempt -eq $max_attempts ]; then
  echo "❌ Superset failed to start within expected time"
  exit 1
fi

# Run database migrations
echo "📊 Running database migrations..."
superset db upgrade

# Create admin user (only if it doesn't exist)
echo "👤 Creating admin user..."
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin \
  || echo "⚠️  Admin user creation failed or user already exists"

# Initialize Superset (create roles and permissions)
echo "🔧 Initializing Superset permissions and roles..."
superset init

echo ""
echo "🎉 Superset initialization complete!"
echo ""
echo "📁 DataFusion Sample Data Available:"
echo "   - users.parquet: Sample user data"
echo "   - setup.sql: SQL script to register tables"
echo ""
echo "🔗 To use DataFusion in Superset:"
echo "   1. Go to Data > Databases"
echo "   2. Add Database with URI: datafusion:///app/setup.sql"
echo "   3. Test connection and explore the 'users' table"
echo ""
echo "🌐 Access Superset at: http://localhost:8088"
echo "👤 Login with: admin / admin"
