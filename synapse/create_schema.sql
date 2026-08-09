/* =============================================================================
   create_schema.sql
   Purpose : Create the bronze, silver, and gold schemas in the Azure Synapse
             dedicated SQL pool, plus a lightweight audit schema used by
             Azure Data Factory to log pipeline execution status.
   Run as  : Database admin against the ecommerce_sqlpool dedicated SQL pool.
   ============================================================================= */

-- -----------------------------------------------------------------------------
-- 1. Core medallion schemas
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze AUTHORIZATION dbo');
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver AUTHORIZATION dbo');
END
GO

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold AUTHORIZATION dbo');
END
GO

-- -----------------------------------------------------------------------------
-- 2. Audit schema for ADF pipeline execution logging
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'audit')
BEGIN
    EXEC('CREATE SCHEMA audit AUTHORIZATION dbo');
END
GO

IF NOT EXISTS (
    SELECT * FROM sys.tables t
    JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE s.name = 'audit' AND t.name = 'pipeline_execution_log'
)
BEGIN
    CREATE TABLE audit.pipeline_execution_log
    (
        log_id              BIGINT IDENTITY(1,1) NOT NULL,
        pipeline_run_id     VARCHAR(100)   NOT NULL,
        pipeline_name       VARCHAR(200)   NOT NULL,
        activity_name       VARCHAR(200)   NOT NULL,
        status              VARCHAR(20)    NOT NULL,
        start_time          DATETIME2      NOT NULL,
        end_time            DATETIME2      NULL,
        rows_processed      INT            NULL,
        error_message       VARCHAR(4000)  NULL
    )
    WITH
    (
        DISTRIBUTION = ROUND_ROBIN,
        HEAP
    );
END
GO

-- -----------------------------------------------------------------------------
-- 3. Master credential + database scoped credential for ADLS Gen2 access
--    (required before creating external data sources / file formats)
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.symmetric_keys WHERE name = '##MS_DatabaseMasterKey##')
BEGIN
    CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Ch@ngeMe_Synapse_2024!';
END
GO

IF NOT EXISTS (SELECT * FROM sys.database_scoped_credentials WHERE name = 'ADLS_Credential')
BEGIN
    CREATE DATABASE SCOPED CREDENTIAL ADLS_Credential
    WITH
        IDENTITY = 'Managed Identity';
END
GO

PRINT 'Schemas (bronze, silver, gold, audit) and audit table created successfully.';
