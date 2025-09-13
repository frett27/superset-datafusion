#!/bin/bash
set -e

echo "🚀 Initializing Superset with DataFusion support..."

# Wait for Superset to be ready
echo "⏳ Waiting for Superset to be ready..."
until curl -f http://localhost:8088/health >/dev/null 2>&1; do
  echo "Waiting for Superset..."
  sleep 5
done

echo "✅ Superset is ready!"

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
  || echo "Admin user already exists"

# Initialize Superset
echo "🔧 Initializing Superset permissions and roles..."
superset init

echo "🎉 Superset initialization complete!"
echo "🌐 Access Superset at: http://localhost:8088"
echo "👤 Login with: admin / admin"
