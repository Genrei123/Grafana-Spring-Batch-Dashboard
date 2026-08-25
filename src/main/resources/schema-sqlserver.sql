-- Boot loads this instead of schema.sql when spring.sql.init.platform=sqlserver
-- (see application.yaml) - i.e. for the real app run against the Dockerized
-- SQL Server, not the H2 tests use. IF OBJECT_ID guards make it idempotent
-- across app restarts against the same persistent database, since T-SQL has
-- no "CREATE TABLE IF NOT EXISTS". Spring Batch's own
-- BATCH_JOB_INSTANCE/BATCH_STEP_EXECUTION/... tables are created separately
-- by db/batch-schema-sqlserver.sql - see spring.batch.jdbc.schema.
IF OBJECT_ID('dbo.customer', 'U') IS NULL
CREATE TABLE customer (
    id    BIGINT PRIMARY KEY,
    name  VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL
);
