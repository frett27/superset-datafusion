-- DataFusion setup script for Superset integration
-- This script registers Parquet files as tables in DataFusion

-- Register users table from the users.parquet file
CREATE EXTERNAL TABLE users (
    id BIGINT,
    name VARCHAR,
    email VARCHAR,
    age INTEGER,
    city VARCHAR,
    country VARCHAR,
    created_at TIMESTAMP
) STORED AS PARQUET LOCATION '/app/users.parquet';

-- Create some sample views for demonstration
CREATE VIEW active_users AS
SELECT 
    id,
    name,
    email,
    age,
    city,
    country,
    created_at
FROM users
WHERE age >= 18;

CREATE VIEW users_by_country AS
SELECT 
    country,
    COUNT(*) as user_count,
    AVG(age) as avg_age,
    MIN(created_at) as first_user,
    MAX(created_at) as latest_user
FROM users
GROUP BY country
ORDER BY user_count DESC;

CREATE VIEW users_by_city AS
SELECT 
    city,
    country,
    COUNT(*) as user_count,
    AVG(age) as avg_age
FROM users
GROUP BY city, country
ORDER BY user_count DESC;

-- Show registered tables
SHOW TABLES;
