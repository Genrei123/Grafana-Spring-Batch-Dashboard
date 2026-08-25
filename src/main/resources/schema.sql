-- Runs automatically against embedded H2 in tests (Boot's default
-- spring.sql.init.mode=embedded - see src/test/resources/application.yaml,
-- which doesn't override it). H2 supports "CREATE TABLE IF NOT EXISTS"
-- directly, unlike T-SQL - see schema-sqlserver.sql, which Boot uses instead
-- of this file for the real app run (spring.sql.init.platform=sqlserver in
-- application.yaml). Spring Batch's own BATCH_JOB_INSTANCE/BATCH_STEP_EXECUTION/...
-- tables are created separately and automatically by spring-boot-starter-batch
-- for the same reason - no config needed for those in the H2/test case.
CREATE TABLE IF NOT EXISTS customer (
    id    BIGINT PRIMARY KEY,
    name  VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL
);
