# Superset with DataFusion Integration

This Docker Compose setup provides Apache Superset with DataFusion support out of the box, including sample data.

## Quick Start (Recommended)

**Option 1: Automatic initialization**
```bash
./start_with_init.sh
```

**Option 2: Manual steps**
```bash
# Start services
docker compose up -d

# Wait for Superset to be ready, then initialize
docker compose exec superset /app/init_superset.sh
```

## What the Initialization Does

The initialization script automatically:
- ✅ Waits for Superset to be ready
- ✅ Runs `superset db upgrade` (database migrations)
- ✅ Creates admin user (`admin` / `admin`)
- ✅ Runs `superset init` (permissions and roles)
- ✅ Sets up DataFusion integration

## Sample Data Included

- **`users.parquet`**: Sample user dataset
- **`setup.sql`**: SQL script that registers the Parquet file as a DataFusion table

## Using DataFusion in Superset

1. **Add DataFusion Database:**
   - Go to Data > Databases
   - Click "Add Database"
   - Use URI: `datafusion:///app/setup.sql`
   - Test connection

2. **Explore Data:**
   - The `users` table will be available
   - Sample views: `active_users`, `users_by_country`, `users_by_city`

## Services

- **mysql**: MySQL 8.0 database for Superset metadata
- **superset**: Apache Superset with DataFusion support

## Access

- **URL**: http://localhost:8088
- **Username**: `admin`
- **Password**: `admin`

## Troubleshooting

**Check if initialization is needed:**
```bash
# Check if admin user exists
docker compose exec superset superset fab list-users

# Run initialization manually
docker compose exec superset /app/init_superset.sh
```

**View logs:**
```bash
docker compose logs superset
```

**Reset everything:**
```bash
docker compose down -v  # Removes volumes too
./start_with_init.sh
```
