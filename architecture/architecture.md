# Architecture

This document explains the end-to-end architecture of the **Azure E-Commerce Data Pipeline**, a medallion-architecture solution built for a simulated Brazilian e-commerce company. It covers every hop the data takes — from raw source files to business-ready KPIs — and the Azure service responsible for each stage.

A visual version of this diagram is available in [`architecture.drawio`](./architecture.drawio) (open with [draw.io](https://app.diagrams.net/) or the VS Code Draw.io extension).

---

## 1. High-Level Flow

```
 Source CSVs            Azure Data         Azure Data Lake        Azure Databricks         Azure Synapse           Business Layer
 (landing zone)    -->  Factory (ADF)  --> Storage Gen2 (ADLS) --> (PySpark)         -->    Analytics         -->  (Power BI /
                        orchestration       Bronze/Silver/Gold      transformation          external tables          reporting)
                                                                                              & views
```

| Stage | Azure Service | Responsibility |
|---|---|---|
| 1 | Source landing zone | Raw CSV files (customers, orders, products, order_items, sellers) land in a blob container |
| 2 | Azure Data Factory | Orchestrates ingestion, copies CSVs to Bronze, triggers Databricks notebooks in sequence, logs run status |
| 3 | Azure Data Lake Storage Gen2 | Hierarchical namespace storage for Bronze / Silver / Gold Parquet data |
| 4 | Azure Databricks (PySpark) | Executes the Bronze → Silver → Gold transformation chain and computes business KPIs |
| 5 | Azure Synapse Analytics | Exposes Gold layer Parquet as external tables and analytical views for SQL consumption |
| 6 | Business / Reporting Layer | Power BI, Excel, or any SQL client connects to Synapse views for dashboards and ad hoc analysis |

---

## 2. Source Layer

The pipeline simulates the **Brazilian E-Commerce public dataset** structure with five core entities:

- `customers.csv` — customer demographic and signup data
- `orders.csv` — order header data (status, payment value, order date)
- `order_items.csv` — line-item detail per order (product, seller, quantity, freight)
- `products.csv` — product catalog with category and price
- `sellers.csv` — marketplace seller directory

These files are dropped into a `raw` container under `landing/ecommerce/` in ADLS Gen2, simulating a daily batch drop from an upstream e-commerce platform or ERP export.

---

## 3. Azure Data Factory (Orchestration Layer)

`datafactory/adf_pipeline.json` defines the master pipeline **`PL_Ecommerce_Bronze_Ingestion`**, which:

1. **`ForEach_Source_Entity`** — iterates over all five source entities in parallel (batch count = 5) and copies each CSV into the Bronze container as Parquet using a `Copy` activity with schema-aware type conversion.
2. **`Notebook_Bronze_Layer`** — invokes the Databricks Bronze notebook to validate schema and stamp ingestion metadata.
3. **`Notebook_Silver_Layer`** — invokes the Silver notebook to clean, deduplicate, and standardize the data.
4. **`Notebook_Gold_Layer`** — invokes the Gold notebook to build the three business marts.
5. **`Log_Pipeline_Success` / `Log_Pipeline_Failure`** — writes a row to `audit.pipeline_execution_log` in Synapse so every run is auditable, regardless of outcome.

Supporting configuration:
- `linked_service.json` — connections to ADLS Gen2, Azure Key Vault (for secrets), Azure Databricks, and Azure Synapse.
- `datasets.json` — the dataset definitions for each source CSV, the parameterized Bronze Parquet sink, and the Synapse audit log table.

All credentials are referenced via **Azure Key Vault** — no secrets are hard-coded in the pipeline JSON.

---

## 4. Azure Data Lake Storage Gen2 (Storage Layer)

ADLS Gen2 hosts the **medallion architecture** using a hierarchical namespace for efficient directory-level operations:

```
datalake/
├── bronze/
│   ├── customers/   ├── orders/   ├── products/   ├── order_items/   ├── sellers/
├── silver/
│   ├── customers/   ├── orders/   ├── products/   ├── order_items/   ├── sellers/
└── gold/
    ├── customer_sales_summary/
    ├── product_performance/
    ├── seller_performance/
    └── kpis/
        ├── total_revenue/        ├── average_order_value/
        ├── top_10_products/      ├── top_10_customers/
        ├── top_sellers/          ├── monthly_revenue_trend/
        └── repeat_customer_rate/
```

All data is stored in **Parquet** with Snappy compression for efficient columnar storage and fast Synapse external-table reads.

---

## 5. Azure Databricks (Processing Layer)

PySpark notebooks under `/databricks` implement the transformation logic:

- **`bronze_layer.py`** — reads raw CSV with enforced schemas, validates required columns, stamps `_ingestion_timestamp` / `_source_file` / `_pipeline_run_id`, writes partitioned Parquet.
- **`silver_layer.py`** — deduplicates by business key (keeping the most recently ingested row), handles nulls, standardizes date formats to `DateType`, normalizes category names to snake_case, validates Brazilian state codes, and flags row-level data-quality issues via `_quality_flag`.
- **`gold_layer.py`** — builds the three business marts: Customer Sales Summary, Product Performance, and Seller Performance, filtering to "completed" order statuses (`delivered`, `shipped`, `invoiced`).
- **`transformations.py`** — shared, reusable utility functions (dedupe, null-fill, type casting, text normalization, audit-column stamping) imported by the other notebooks to avoid logic duplication.
- **`business_kpis.py`** — computes Total Revenue, Average Order Value, Top 10 Products, Top 10 Customers, Top Sellers, Monthly Revenue Trend, and Repeat Customer Rate, writing each as its own Parquet KPI table.

Notebooks are parameterized via `dbutils.widgets` so ADF can pass `run_date`, source/target paths at trigger time, enabling reuse across dev/test/prod environments.

---

## 6. Azure Synapse Analytics (Serving Layer)

`/synapse` contains the SQL DDL that exposes Gold layer Parquet to SQL consumers without copying data:

- **`create_schema.sql`** — creates `bronze`, `silver`, `gold`, and `audit` schemas plus the `audit.pipeline_execution_log` table, and the database-scoped credential used for ADLS authentication via managed identity.
- **`create_external_tables.sql`** — defines the `ParquetFileFormat`, the `GoldDataLake` external data source, and external tables over each Gold Parquet output.
- **`gold_views.sql`** — business-friendly views (`vw_customer_sales`, `vw_product_performance`, `vw_seller_performance`, `vw_monthly_revenue_trend`) with derived metrics like customer segments, revenue rank, and quartiles.
- **`analytical_queries.sql`** — 20 ready-to-run business queries covering revenue, top performers, state-level breakdowns, repeat-customer analysis, and an executive KPI snapshot.

---

## 7. Business / Reporting Layer

Any SQL-speaking BI tool (Power BI, Tableau, Excel, or a SQL client) connects to the Synapse dedicated SQL pool and queries the `gold.vw_*` views directly. Because the views sit on top of external tables, **the underlying Parquet files can be reprocessed and refreshed without changing a single line of downstream reporting SQL.**

---

## 8. Security & Governance Notes

- All secrets (storage account keys, Databricks PAT, Synapse SQL password) are stored in **Azure Key Vault** and referenced by ADF linked services — never embedded in plain text.
- ADLS Gen2 access from Synapse uses a **managed identity** database-scoped credential rather than account keys.
- Every pipeline run is logged to `audit.pipeline_execution_log`, giving full lineage of run ID, status, row counts, and error messages for troubleshooting and SLA monitoring.
- Row-level data-quality flags (`_quality_flag`) in the Silver layer allow downstream consumers to filter to `VALID` records only, while preserving rejected/flagged rows for data-quality review rather than silently dropping them.
