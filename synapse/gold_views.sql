/* =============================================================================
   gold_views.sql
   Purpose : Create business-friendly views over the Gold layer external
             tables. These views are what Power BI / analysts query directly
             so the underlying Parquet/external-table structure can evolve
             without breaking downstream reports.
   ============================================================================= */

-- -----------------------------------------------------------------------------
-- View 1: vw_customer_sales
-- -----------------------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_customer_sales' AND schema_id = SCHEMA_ID('gold'))
    DROP VIEW gold.vw_customer_sales;
GO

CREATE VIEW gold.vw_customer_sales AS
SELECT
    customer_id,
    customer_name,
    city,
    state,
    total_orders,
    total_spent,
    avg_order_value,
    CASE
        WHEN total_spent >= 5000 THEN 'VIP'
        WHEN total_spent >= 1500 THEN 'High Value'
        WHEN total_spent >= 500  THEN 'Mid Value'
        ELSE 'Low Value'
    END AS customer_segment
FROM gold.ext_customer_sales_summary;
GO

-- -----------------------------------------------------------------------------
-- View 2: vw_product_performance
-- -----------------------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_product_performance' AND schema_id = SCHEMA_ID('gold'))
    DROP VIEW gold.vw_product_performance;
GO

CREATE VIEW gold.vw_product_performance AS
SELECT
    product_id,
    product_name,
    category,
    revenue,
    units_sold,
    CASE WHEN units_sold > 0 THEN ROUND(revenue / units_sold, 2) ELSE 0 END AS revenue_per_unit,
    RANK() OVER (ORDER BY revenue DESC) AS revenue_rank,
    RANK() OVER (PARTITION BY category ORDER BY revenue DESC) AS category_revenue_rank
FROM gold.ext_product_performance;
GO

-- -----------------------------------------------------------------------------
-- View 3: vw_seller_performance
-- -----------------------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_seller_performance' AND schema_id = SCHEMA_ID('gold'))
    DROP VIEW gold.vw_seller_performance;
GO

CREATE VIEW gold.vw_seller_performance AS
SELECT
    seller_id,
    seller_name,
    state,
    revenue,
    orders_processed,
    CASE WHEN orders_processed > 0 THEN ROUND(revenue / orders_processed, 2) ELSE 0 END AS revenue_per_order,
    RANK() OVER (ORDER BY revenue DESC) AS revenue_rank,
    NTILE(4) OVER (ORDER BY revenue DESC) AS performance_quartile
FROM gold.ext_seller_performance;
GO

-- -----------------------------------------------------------------------------
-- View 4: vw_monthly_revenue_trend (supporting view referenced by analytical_queries.sql)
-- -----------------------------------------------------------------------------
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_monthly_revenue_trend' AND schema_id = SCHEMA_ID('gold'))
    DROP VIEW gold.vw_monthly_revenue_trend;
GO

CREATE VIEW gold.vw_monthly_revenue_trend AS
SELECT
    order_month,
    monthly_revenue,
    order_count,
    ROUND(monthly_revenue / NULLIF(order_count, 0), 2) AS avg_order_value,
    LAG(monthly_revenue) OVER (ORDER BY order_month) AS prior_month_revenue,
    ROUND(
        100.0 * (monthly_revenue - LAG(monthly_revenue) OVER (ORDER BY order_month))
        / NULLIF(LAG(monthly_revenue) OVER (ORDER BY order_month), 0), 2
    ) AS pct_change_vs_prior_month
FROM gold.ext_monthly_revenue_trend;
GO

PRINT 'Gold layer analytical views created successfully: vw_customer_sales, vw_product_performance, vw_seller_performance, vw_monthly_revenue_trend.';
