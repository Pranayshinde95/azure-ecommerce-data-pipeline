/* =============================================================================
   analytical_queries.sql
   Purpose : 20 production-style business analytical queries against the Gold
             layer views/external tables. Grouped by theme. Run these in
             Synapse Studio or any SQL client connected to the dedicated pool.
   ============================================================================= */

-- =============================================================================
-- 1. Total revenue across the business
-- =============================================================================
SELECT ROUND(SUM(revenue), 2) AS total_revenue
FROM gold.ext_product_performance;
GO

-- =============================================================================
-- 2. Average order value across all customers
-- =============================================================================
SELECT ROUND(AVG(avg_order_value), 2) AS overall_avg_order_value
FROM gold.vw_customer_sales;
GO

-- =============================================================================
-- 3. Monthly revenue trend, most recent month first
-- =============================================================================
SELECT order_month, monthly_revenue, order_count, pct_change_vs_prior_month
FROM gold.vw_monthly_revenue_trend
ORDER BY order_month DESC;
GO

-- =============================================================================
-- 4. Top 10 highest-revenue products
-- =============================================================================
SELECT TOP 10 product_id, product_name, category, revenue, units_sold
FROM gold.vw_product_performance
ORDER BY revenue DESC;
GO

-- =============================================================================
-- 5. Top 10 customers by total spend
-- =============================================================================
SELECT TOP 10 customer_id, customer_name, city, state, total_spent, total_orders
FROM gold.vw_customer_sales
ORDER BY total_spent DESC;
GO

-- =============================================================================
-- 6. Top performing sellers by revenue
-- =============================================================================
SELECT TOP 10 seller_id, seller_name, state, revenue, orders_processed
FROM gold.vw_seller_performance
ORDER BY revenue DESC;
GO

-- =============================================================================
-- 7. Revenue grouped by Brazilian state (highest revenue state)
-- =============================================================================
SELECT state, ROUND(SUM(total_spent), 2) AS state_revenue, COUNT(DISTINCT customer_id) AS customer_count
FROM gold.vw_customer_sales
GROUP BY state
ORDER BY state_revenue DESC;
GO

-- =============================================================================
-- 8. Highest single revenue-generating state
-- =============================================================================
SELECT TOP 1 state, ROUND(SUM(total_spent), 2) AS state_revenue
FROM gold.vw_customer_sales
GROUP BY state
ORDER BY state_revenue DESC;
GO

-- =============================================================================
-- 9. Revenue and unit sales broken down by product category
-- =============================================================================
SELECT category, ROUND(SUM(revenue), 2) AS category_revenue, SUM(units_sold) AS category_units_sold
FROM gold.vw_product_performance
GROUP BY category
ORDER BY category_revenue DESC;
GO

-- =============================================================================
-- 10. Repeat customers: customers with more than one order
-- =============================================================================
SELECT customer_id, customer_name, total_orders, total_spent
FROM gold.vw_customer_sales
WHERE total_orders > 1
ORDER BY total_orders DESC, total_spent DESC;
GO

-- =============================================================================
-- 11. Repeat customer rate (% of customers with more than one order)
-- =============================================================================
SELECT
    COUNT(CASE WHEN total_orders > 1 THEN 1 END) AS repeat_customers,
    COUNT(*) AS total_customers,
    ROUND(100.0 * COUNT(CASE WHEN total_orders > 1 THEN 1 END) / COUNT(*), 2) AS repeat_customer_rate_pct
FROM gold.vw_customer_sales;
GO

-- =============================================================================
-- 12. Customer segmentation distribution (VIP / High / Mid / Low value)
-- =============================================================================
SELECT customer_segment, COUNT(*) AS customer_count, ROUND(SUM(total_spent), 2) AS segment_revenue
FROM gold.vw_customer_sales
GROUP BY customer_segment
ORDER BY segment_revenue DESC;
GO

-- =============================================================================
-- 13. Revenue contribution by seller, ranked into quartiles
-- =============================================================================
SELECT performance_quartile, COUNT(*) AS seller_count, ROUND(SUM(revenue), 2) AS quartile_revenue
FROM gold.vw_seller_performance
GROUP BY performance_quartile
ORDER BY performance_quartile;
GO

-- =============================================================================
-- 14. Products with above-average revenue per unit (premium products)
-- =============================================================================
SELECT product_id, product_name, category, revenue_per_unit
FROM gold.vw_product_performance
WHERE revenue_per_unit > (SELECT AVG(revenue_per_unit) FROM gold.vw_product_performance)
ORDER BY revenue_per_unit DESC;
GO

-- =============================================================================
-- 15. Category leaders: #1 revenue product within each category
-- =============================================================================
SELECT product_id, product_name, category, revenue
FROM gold.vw_product_performance
WHERE category_revenue_rank = 1
ORDER BY revenue DESC;
GO

-- =============================================================================
-- 16. Sellers processing the most orders (operational throughput)
-- =============================================================================
SELECT TOP 10 seller_id, seller_name, orders_processed, revenue_per_order
FROM gold.vw_seller_performance
ORDER BY orders_processed DESC;
GO

-- =============================================================================
-- 17. Month-over-month revenue growth/decline flags
-- =============================================================================
SELECT
    order_month,
    monthly_revenue,
    pct_change_vs_prior_month,
    CASE
        WHEN pct_change_vs_prior_month > 0 THEN 'Growth'
        WHEN pct_change_vs_prior_month < 0 THEN 'Decline'
        ELSE 'Flat / No Prior Data'
    END AS trend_direction
FROM gold.vw_monthly_revenue_trend
ORDER BY order_month;
GO

-- =============================================================================
-- 18. Average revenue per customer by state, sorted descending
-- =============================================================================
SELECT
    state,
    COUNT(DISTINCT customer_id) AS customers,
    ROUND(SUM(total_spent) / COUNT(DISTINCT customer_id), 2) AS avg_revenue_per_customer
FROM gold.vw_customer_sales
GROUP BY state
ORDER BY avg_revenue_per_customer DESC;
GO

-- =============================================================================
-- 19. Bottom 10 underperforming products (candidates for promotion/clearance)
-- =============================================================================
SELECT TOP 10 product_id, product_name, category, revenue, units_sold
FROM gold.vw_product_performance
ORDER BY revenue ASC;
GO

-- =============================================================================
-- 20. Combined executive summary: single-row KPI snapshot
-- =============================================================================
SELECT
    (SELECT ROUND(SUM(revenue), 2) FROM gold.ext_product_performance) AS total_revenue,
    (SELECT ROUND(AVG(avg_order_value), 2) FROM gold.vw_customer_sales) AS avg_order_value,
    (SELECT COUNT(DISTINCT customer_id) FROM gold.vw_customer_sales) AS total_customers,
    (SELECT COUNT(DISTINCT seller_id) FROM gold.vw_seller_performance) AS total_sellers,
    (SELECT COUNT(DISTINCT product_id) FROM gold.vw_product_performance) AS total_products_sold;
GO
