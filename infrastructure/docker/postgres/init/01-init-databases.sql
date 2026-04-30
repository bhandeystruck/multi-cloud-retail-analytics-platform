-- ============================================================
-- PostgreSQL initialization script
--
-- Why this file exists:
-- The local PostgreSQL container is used for two purposes:
--
-- 1. retail_analytics:
--    Simulates the analytical warehouse that will later map to
--    Redshift and BigQuery-style schemas.
--
-- 2. airflow:
--    Stores Airflow metadata such as DAG runs, task instances,
--    users, variables, and scheduler state.
--
-- This script runs automatically when the PostgreSQL container
-- is created for the first time.
-- ============================================================


-- ------------------------------------------------------------
-- Create Airflow user if it does not already exist.
-- ------------------------------------------------------------

DO
$$
BEGIN
   IF NOT EXISTS (
      SELECT FROM pg_catalog.pg_roles WHERE rolname = 'airflow'
   ) THEN
      CREATE ROLE airflow LOGIN PASSWORD 'airflow';
   END IF;
END
$$;


-- ------------------------------------------------------------
-- Create Airflow metadata database.
-- ------------------------------------------------------------

SELECT 'CREATE DATABASE airflow OWNER airflow'
WHERE NOT EXISTS (
   SELECT FROM pg_database WHERE datname = 'airflow'
)\gexec


-- ------------------------------------------------------------
-- Grant privileges.
-- ------------------------------------------------------------

GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;