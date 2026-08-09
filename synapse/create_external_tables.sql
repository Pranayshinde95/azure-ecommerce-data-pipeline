/* =============================================================================
   create_external_tables.sql
   Purpose : Define the external data source / file format pointing at ADLS
             Gen2, then create external tables over the Gold layer Parquet
             files so they can be queried directly from Synapse serverless
             or dedicated SQL pools without duplicating storage.
   ============================================================================= */

-- -----------------------------------------------------------------------------
-- 1. External file format (Parquet, Snappy compression)
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'ParquetFileFormat')
BEGIN
    CREATE EXTERNAL FILE FORMAT ParquetFileFormat
    WITH
    (
        FORMAT_TYPE = PARQUET,
        DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
    );
END
GO

-- -----------------------------------------------------------------------------
-- 2. External data source pointing at the Gold container in ADLS Gen2
-- -----------------------------------------------------------------------------
IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'GoldDataLake')
BEGIN
    CREATE EXTERNAL DATA SOURCE GoldDataLake
    WITH
    (
        LOCATION   = 'abfss://datalake@ecommercedatalakedev.dfs.core.windows.net/gold',
        CREDENTIAL = ADLS_Credential
    );
END
GO

-- -----------------------------------------------------------------------------
-- 3. External Table: gold.ext_customer_sales_summary
-- -----------------------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'ext_customer_sales_summary' AND schema_id = SCHEMA_ID('gold'))
    DROP EXTERNAL TABLE gold.ext_customer_sales_summary;
GO

CREATE EXTERNAL TABLE gold.ext_customer_sales_summary
(
    customer_id     VARCHAR(20)     NOT NULL,
    customer_name   VARCHAR(200)    NULL,
    city            VARCHAR(100)    NULL,
    state           VARCHAR(5)      NULL,
    total_orders    INT             NULL,
    total_spent     DECIMAL(18,2)   NULL,
    avg_order_value DECIMAL(18,2)   NULL
)
WITH
(
    LOCATION       = '/customer_sales_summary/',
    DATA_SOURCE    = GoldDataLake,
    FILE_FORMAT    = ParquetFileFormat
);
GO

-- -----------------------------------------------------------------------------
-- 4. External Table: gold.ext_product_performance
-- -----------------------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'ext_product_performance' AND schema_id = SCHEMA_ID('gold'))
    DROP EXTERNAL TABLE gold.ext_product_performance;
GO

CREATE EXTERNAL TABLE gold.ext_product_performance
(
    product_id      VARCHAR(20)     NOT NULL,
    product_name    VARCHAR(300)    NULL,
    category        VARCHAR(100)    NULL,
    revenue         DECIMAL(18,2)   NULL,
    units_sold      INT             NULL
)
WITH
(
    LOCATION       = '/product_performance/',
    DATA_SOURCE    = GoldDataLake,
    FILE_FORMAT    = ParquetFileFormat
);
GO

-- -----------------------------------------------------------------------------
-- 5. External Table: gold.ext_seller_performance
-- -----------------------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'ext_seller_performance' AND schema_id = SCHEMA_ID('gold'))
    DROP EXTERNAL TABLE gold.ext_seller_performance;
GO

CREATE EXTERNAL TABLE gold.ext_seller_performance
(
    seller_id           VARCHAR(20)     NOT NULL,
    seller_name         VARCHAR(200)    NULL,
    state               VARCHAR(5)      NULL,
    revenue             DECIMAL(18,2)   NULL,
    orders_processed    INT             NULL
)
WITH
(
    LOCATION       = '/seller_performance/',
    DATA_SOURCE    = GoldDataLake,
    FILE_FORMAT    = ParquetFileFormat
);
GO

-- -----------------------------------------------------------------------------
-- 6. External Table: gold.ext_monthly_revenue_trend (from business_kpis.py)
-- -----------------------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'ext_monthly_revenue_trend' AND schema_id = SCHEMA_ID('gold'))
    DROP EXTERNAL TABLE gold.ext_monthly_revenue_trend;
GO

CREATE EXTERNAL TABLE gold.ext_monthly_revenue_trend
(
    order_month     VARCHAR(7)      NOT NULL,
    monthly_revenue DECIMAL(18,2)   NULL,
    order_count     INT             NULL,
    run_date        VARCHAR(20)     NULL
)
WITH
(
    LOCATION       = '/kpis/monthly_revenue_trend/',
    DATA_SOURCE    = GoldDataLake,
    FILE_FORMAT    = ParquetFileFormat
);
GO

PRINT 'External data source, file format, and Gold layer external tables created successfully.';
