-- Separate database for Airflow's metadata (used in the orchestration step).
-- Keeps Airflow internals apart from our business tables in the "fleet" database.
CREATE DATABASE airflow;