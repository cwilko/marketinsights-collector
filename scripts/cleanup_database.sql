-- Database cleanup script for econometrics pipeline
-- Run this as PostgreSQL superuser (e.g., postgres user)
-- 
-- Environment variables (set these before running):
-- ECON_DB_NAME (default: econometrics)
-- ECON_DB_USER (default: econometrics_user)
--
-- Example usage:
-- psql -U postgres -v db_name="econometrics" -v db_user="econometrics_user" -f scripts/cleanup_database.sql
-- 
-- Or with environment variables:
-- psql -U postgres -v db_name="$ECON_DB_NAME" -v db_user="$ECON_DB_USER" -f scripts/cleanup_database.sql
--
-- WARNING: This will permanently delete the database and all data!

-- Set default values if environment variables are not set
-- \set db_name `echo ${ECON_DB_NAME:-econometrics}`
-- \set db_user `echo ${ECON_DB_USER:-econometrics_user}`

-- Display what we're removing
\echo '⚠️  WARNING: This will permanently delete:'
\echo 'Database name:' :db_name
\echo 'User name:' :db_user
\echo ''
\echo 'All data will be lost permanently!'
\echo ''
\echo 'Proceeding with deletion...'
\echo ''

-- Terminate all connections to the database
SELECT 'SELECT pg_terminate_backend(pg_stat_activity.pid) FROM pg_stat_activity WHERE pg_stat_activity.datname = ''' || :'db_name' || ''' AND pid <> pg_backend_pid();' AS terminate_connections \gset
:terminate_connections

-- Drop database if it exists
SELECT 'DROP DATABASE IF EXISTS ' || :'db_name' || ';' AS drop_db \gset
:drop_db

-- Drop user if it exists (and has no dependencies)  
SELECT 'DROP USER IF EXISTS ' || :'db_user' || ';' AS drop_user \gset
:drop_user

-- Display success message
\echo ''
\echo '✅ Database cleanup completed successfully!'
\echo 'Removed database:' :db_name
\echo 'Removed user:' :db_user
\echo ''
\echo 'You can now remove these from your .env file:'
\echo 'DATABASE_URL=postgresql://...'