# Project Overview

## What This Project Is

The **Azure E-Commerce Data Pipeline** is an end-to-end, production-style Azure Data Engineering solution built around a simulated Brazilian e-commerce dataset. It demonstrates how raw transactional CSV exports become governed, query-ready business data using the **medallion architecture** (Bronze → Silver → Gold) on Azure.

The project is intentionally structured the way a real engineering team would organize it: orchestration config separate from compute logic, compute logic separate from the serving/SQL layer, and documentation that explains *why*, not just *what*.

## Business Problem Being Solved

An e-commerce marketplace generates five core data feeds daily: customers, orders, order line items, the product catalog, and the seller directory. Raw, these feeds are:

- Inconsistent (mixed date formats, inconsistent state codes, duplicate records from retries)
- Not analysis-ready (no joins, no aggregations, no business definitions applied)
- Not auditable (no record of when data arrived, from where, or whether it passed quality checks)

This pipeline solves that by automating ingestion, applying consistent data-quality rules, and publishing trustworthy, pre-aggregated business marts that analysts and BI tools can query directly — without ever touching raw files.

## Who Would Use This

- **Data Analysts / BI Developers** — query `gold.vw_customer_sales`, `gold.vw_product_performance`, `gold.vw_seller_performance` directly from Power BI or any SQL client.
- **Data Engineers** — extend the Bronze/Silver/Gold notebooks, add new source entities, or wire in real-time ingestion using the same patterns.
- **Engineering Managers / Recruiters** — review this repo as a portfolio piece demonstrating Azure Data Factory orchestration, Databricks/PySpark transformation patterns, Synapse external tables, and medallion architecture design.

## What "Production-Style" Means Here

- **Schema enforcement** at ingestion (Bronze) rather than discovering bad data downstream.
- **Idempotent writes** (`mode("overwrite")` per partition/entity) so re-running a pipeline doesn't duplicate data.
- **Row-level data-quality flags** instead of silently dropping bad records — Silver keeps everything but tags it.
- **Externalized credentials** via Azure Key Vault — nothing sensitive is hard-coded.
- **Execution auditing** — every ADF run logs status, row counts, and errors to a Synapse audit table.
- **Parameterized notebooks** — the same Databricks code runs across dev/test/prod by changing widget values, not code.

## Repository Map

| Folder | Contents |
|---|---|
| `datasets/` | Sample source CSVs (customers, orders, products, order_items, sellers) |
| `datafactory/` | ADF linked services, dataset definitions, and the master pipeline JSON |
| `databricks/` | PySpark notebooks for Bronze, Silver, Gold, shared transformations, and business KPIs |
| `synapse/` | SQL DDL for schemas, external tables, Gold views, and 20 analytical queries |
| `architecture/` | Architecture explanation (markdown) and a draw.io diagram |
| `screenshots/` | Checklist for execution-evidence screenshots once deployed |
| `docs/` | This file, the medallion architecture explainer, and the deployment guide |

See [`architecture/architecture.md`](../architecture/architecture.md) for the full technical walkthrough and [`docs/deployment_guide.md`](./deployment_guide.md) to actually stand this up in an Azure subscription.
