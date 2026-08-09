# Screenshots

This folder is where execution-evidence screenshots should live once the
pipeline is deployed and run against a real Azure subscription:

| File | What to capture |
|---|---|
| `adf_pipeline.png` | Azure Data Factory Studio — the `PL_Ecommerce_Bronze_Ingestion` pipeline canvas with a successful run (green checkmarks) in the **Monitor** tab |
| `databricks_execution.png` | Databricks notebook run output for `bronze_layer.py` / `silver_layer.py` / `gold_layer.py`, showing the print summaries and `dbutils.notebook.exit()` result |
| `synapse_queries.png` | Synapse Studio SQL script results for a few queries from `analytical_queries.sql` (e.g., Top 10 Products, Monthly Revenue Trend) |
| `dashboard.png` | A Power BI / Synapse dashboard built on top of `gold.vw_customer_sales`, `gold.vw_product_performance`, and `gold.vw_seller_performance` |

> **Note:** These are intentionally left as a checklist rather than fabricated
> images — genuine screenshots can only be captured by actually running this
> project against a live Azure environment (see `docs/deployment_guide.md`).
> Once you deploy the pipeline, replace this file's checklist with the real
> `.png` captures so the README's screenshot section renders correctly.
