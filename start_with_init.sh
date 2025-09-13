#!/bin/bash
set -e

echo "🚀 Starting Superset with DataFusion integration..."

# Stop any existing containers
echo "🛑 Stopping existing containers..."
docker compose down

# Start services
echo "▶️  Starting services..."
docker compose up -d

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

# Run initialization
echo "🔧 Running Superset initialization..."
docker compose exec superset /app/init_superset.sh

echo ""
echo "�� Setup complete!"
echo "🌐 Access Superset at: http://localhost:8088"
echo "👤 Login with: admin / admin"
echo ""
echo "📊 To view logs: docker compose logs -f"
echo "🛑 To stop: docker compose down"
