-- Database setup script for econometrics pipeline
-- Run this as PostgreSQL superuser (e.g., postgres user)
-- 
-- Environment variables (set these before running):
-- ECON_DB_NAME (default: econometrics)
-- ECON_DB_USER (default: econometrics_user) 
-- ECON_DB_PASSWORD (default: econometrics_password)
--
-- Example usage:
-- export ECON_DB_NAME=my_econometrics
-- export ECON_DB_USER=econ_user
-- export ECON_DB_PASSWORD=secure_password
-- psql -U postgres -f scripts/setup_database.sql

-- Set default values if environment variables are not set
-- \set db_name `echo ${ECON_DB_NAME}`
-- \set db_user `echo ${ECON_DB_USER}`
-- \set db_password `echo ${ECON_DB_PASSWORD:-econometrics_password}`

-- Display what we're creating
\echo 'Setting up database with:'
\echo 'Database name:' :db_name
\echo 'User name:' :db_user
\echo 'Password:' :db_password
\echo ''

-- Create database if it doesn't exist
SELECT 'CREATE DATABASE ' || :'db_name'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = :'db_name')\gexec

-- Create user if it doesn't exist
SELECT 'CREATE USER ' || :'db_user' || ' WITH PASSWORD ''' || :'db_password' || ''''
WHERE NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = :'db_user')\gexec

-- Grant all privileges on database to user
SELECT 'GRANT ALL PRIVILEGES ON DATABASE ' || :'db_name' || ' TO ' || :'db_user' || ';'
\gexec

-- Connect to the new database to set up schema permissions
\c :db_name

-- Grant schema permissions
SELECT 'GRANT ALL ON SCHEMA public TO ' || :'db_user' || ';'
\gexec

SELECT 'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ' || :'db_user' || ';'
\gexec

SELECT 'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ' || :'db_user' || ';'
\gexec

-- Set default privileges for future tables and sequences
SELECT 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO ' || :'db_user' || ';'
\gexec

SELECT 'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO ' || :'db_user' || ';'
\gexec

-- Display success message
\echo ''
\echo '✅ Database setup completed successfully!'
\echo 'Database:' :db_name
\echo 'User:' :db_user  
\echo 'Password:' :db_password
\echo ''
\echo 'Add this to your .env file:'
SELECT 'DATABASE_URL=postgresql://' || :'db_user' || ':' || :'db_password' || '@localhost:5432/' || :'db_name' AS database_url \gset
\echo :database_url