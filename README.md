# Azure E-Commerce Data Pipeline

[![Azure](https://img.shields.io/badge/Azure-Data%20Engineering-0078D4?logo=microsoftazure)](https://azure.microsoft.com/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5.1-E25A1C?logo=apachespark)](https://spark.apache.org/)
[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?logo=databricks)](https://databricks.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](#license)

An end-to-end, production-style **Azure Data Engineering pipeline** that ingests a Brazilian e-commerce dataset, processes it through a **medallion architecture** (Bronze → Silver → Gold) using **Azure Data Factory**, **Azure Databricks (PySpark)**, and **Azure Data Lake Storage Gen2**, and exposes business-ready KPIs through **Azure Synapse Analytics**.

---

## Project Overview

E-commerce platforms generate raw, messy transactional data across customers, orders, products, order line items, and sellers. This project simulates a real-world data engineering solution that automates the journey from raw CSV files to trustworthy, query-ready business intelligence — with schema validation, data-quality flagging, lineage metadata, and full pipeline auditing along the way.

Full write-ups are available in [`docs/project_overview.md`](docs/project_overview.md), [`docs/medallion_architecture.md`](docs/medallion_architecture.md), and [`architecture/architecture.md`](architecture/architecture.md).

---

## Architecture Diagram

```
 Source CSVs           Azure Data         Azure Data Lake        Azure Databricks        Azure Synapse          Business Layer
 (landing zone)   -->  Factory (ADF)  --> Storage Gen2 (ADLS) --> (PySpark)        -->    Analytics         -->  (Power BI /
                       orchestration       Bronze/Silver/Gold     transformation          external tables          SQL clients)
                                                                                            & views
```

The full interactive diagram is in [`architecture/architecture.drawio`](architecture/architecture.drawio) — open it with [draw.io](https://app.diagrams.net/) or the Draw.io VS Code extension. Execution-evidence screenshots (ADF run, Databricks output, Synapse query results, dashboard) belong in [`screenshots/`](screenshots/) once deployed — see that folder's checklist.

---

## Tech Stack

| Category | Technology |
|---|---|
| Orchestration | Azure Data Factory |
| Storage | Azure Data Lake Storage Gen2 (hierarchical namespace, Parquet) |
| Processing | Azure Databricks, PySpark |
| Serving / SQL | Azure Synapse Analytics (external tables, views) |
| Secrets Management | Azure Key Vault |
| Languages | Python, SQL |
| Local Tooling | Pandas, Jupyter, pytest |

---

## Folder Structure

```
azure-ecommerce-data-pipeline/
├── datasets/              # Sample source CSVs (customers, orders, products, order_items, sellers)
├── datafactory/            # ADF linked services, dataset definitions, master pipeline
│   ├── adf_pipeline.json
│   ├── linked_service.json
│   └── datasets.json
├── databricks/              # PySpark notebooks: Bronze, Silver, Gold, shared transforms, KPIs
│   ├── bronze_layer.py
│   ├── silver_layer.py
│   ├── gold_layer.py
│   ├── transformations.py
│   └── business_kpis.py
├── synapse/                # SQL DDL: schemas, external tables, views, analytical queries
│   ├── create_schema.sql
│   ├── create_external_tables.sql
│   ├── gold_views.sql
│   └── analytical_queries.sql
├── architecture/            # Architecture explanation + draw.io diagram
│   ├── architecture.md
│   └── architecture.drawio
├── screenshots/              # Execution-evidence screenshot checklist
├── docs/                    # Project overview, medallion explainer, deployment guide
│   ├── project_overview.md
│   ├── medallion_architecture.md
│   └── deployment_guide.md
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Data Flow

1. **Landing Zone** — raw `customers.csv`, `orders.csv`, `products.csv`, `order_items.csv`, `sellers.csv` arrive in an ADLS Gen2 `raw` container.
2. **Azure Data Factory** copies each file into the **Bronze** layer as Parquet, then triggers the Databricks notebook chain.
3. **Bronze (`bronze_layer.py`)** — enforces schema, stamps ingestion metadata (`_ingestion_timestamp`, `_source_file`, `_pipeline_run_id`).
4. **Silver (`silver_layer.py`)** — deduplicates by business key, handles nulls, standardizes dates/categories/state codes, flags row-level quality issues.
5. **Gold (`gold_layer.py`)** — builds Customer Sales Summary, Product Performance, and Seller Performance marts from completed orders only.
6. **`business_kpis.py`** — computes Total Revenue, AOV, Top 10 Products/Customers, Top Sellers, Monthly Revenue Trend, and Repeat Customer Rate.
7. **Azure Synapse Analytics** exposes the Gold Parquet as external tables and business-friendly views (`gold.vw_customer_sales`, `gold.vw_product_performance`, `gold.vw_seller_performance`, `gold.vw_monthly_revenue_trend`).
8. **Business Layer** — Power BI, Excel, or any SQL client queries the Synapse views directly.

Every run is logged to `audit.pipeline_execution_log` in Synapse for full auditability.

---

## KPIs

The pipeline calculates the following business KPIs (see `databricks/business_kpis.py` and `synapse/analytical_queries.sql`):

- **Total Revenue**
- **Average Order Value (AOV)**
- **Top 10 Products** by revenue
- **Top 10 Customers** by spend
- **Top Sellers** by revenue and order volume
- **Monthly Revenue Trend** with month-over-month % change
- **Repeat Customer Rate**

---

## Sample Results

Computed against the sample data shipped in `/datasets` (results will differ once you regenerate or replace the sample CSVs):

| KPI | Value |
|---|---|
| Total Revenue (completed orders) | $589,396.53 |
| Average Order Value | $5,560.34 |
| Completed Orders | 106 of 200 total |
| Repeat Customer Rate | 70.45% (31 of 44 customers) |
| Top Revenue State | RN ($82,512.78) |

**Top 5 Products by Revenue**

| Product | Revenue | Units Sold |
|---|---|---|
| Portable Tablet | $31,438.56 | 24 |
| Eco Wrist Watch | $24,357.00 | 15 |
| Pro High Chair | $23,510.60 | 20 |
| Eco Pet Bed | $22,129.66 | 14 |
| Pro Stapler | $18,550.70 | 10 |

**Top 5 Customers by Spend**

| Customer | State | Total Spent |
|---|---|---|
| Diego Almeida | MA | $42,457.30 |
| Ana Oliveira | RN | $40,122.30 |
| Elisa Monteiro | BA | $37,158.51 |
| Felipe Fernandes | SP | $36,495.05 |
| Vinicius Rocha | PR | $26,939.77 |

**Top 5 Sellers by Revenue**

| Seller | State | Revenue | Orders Processed |
|---|---|---|---|
| Fernandes Shop | MT | $47,082.79 | 12 |
| Carvalho Trading Co | DF | $37,877.45 | 11 |
| Martins Distribuidora | BA | $31,053.80 | 8 |
| Fernandes Mega Loja | RJ | $30,720.70 | 9 |
| Rocha Shop | PR | $29,979.07 | 9 |

---

## How To Run

Full step-by-step instructions are in [`docs/deployment_guide.md`](docs/deployment_guide.md). Summary:

1. **Provision resources** — Resource Group, ADLS Gen2 (with `raw` and `datalake` containers), Key Vault, Azure Databricks, Azure Synapse Analytics, Azure Data Factory.
2. **Upload sample data** — push `/datasets/*.csv` into the ADLS `raw/landing/ecommerce/` path.
3. **Import ADF artifacts** — load `datafactory/linked_service.json`, `datafactory/datasets.json`, and `datafactory/adf_pipeline.json` into ADF Studio; update placeholder resource names.
4. **Attach Databricks Repos** — connect this repository to Databricks Repos so `/databricks/*.py` notebooks resolve at the paths referenced in the ADF pipeline.
5. **Run the pipeline** — trigger `PL_Ecommerce_Bronze_Ingestion` in ADF; it will cascade through Bronze → Silver → Gold automatically.
6. **Build the Synapse layer** — run `synapse/create_schema.sql` → (after Gold data exists) `synapse/create_external_tables.sql` → `synapse/gold_views.sql`.
7. **Query and validate** — run `synapse/analytical_queries.sql` or connect Power BI to the `gold.vw_*` views.

### Local development / testing

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Regenerate or inspect the sample datasets
python3 -c "import pandas as pd; print(pd.read_csv('datasets/orders.csv').head())"
```

---

## Future Enhancements

- Add **Azure Event Hubs / Structured Streaming** for near-real-time order ingestion instead of daily batch.
- Introduce **Great Expectations** or **Delta Live Tables expectations** for declarative data-quality rules beyond the current `_quality_flag` approach.
- Migrate Bronze/Silver/Gold storage to **Delta Lake** format for ACID transactions, time travel, and `MERGE` based upserts.
- Add a **CI/CD pipeline** (GitHub Actions or Azure DevOps) to lint, unit-test PySpark transformations, and deploy ADF/Synapse artifacts automatically.
- Build out a **Power BI dashboard** (`.pbix`) committed alongside `screenshots/dashboard.png` for a fully visual deliverable.
- Add **Unity Catalog** governance for fine-grained access control across Bronze/Silver/Gold.
- Extend KPIs with **cohort retention analysis** and **customer lifetime value (CLV)** modeling.

---

## Resume Bullet Points

ATS-friendly bullet points you can adapt for a Data Engineering resume:

- Architected and deployed an end-to-end Azure data pipeline using **Azure Data Factory, Databricks, and ADLS Gen2**, implementing a **medallion (Bronze/Silver/Gold) architecture** to process e-commerce transactional data at scale.
- Developed **PySpark** ETL pipelines to ingest, validate, deduplicate, and transform 600+ records across 5 relational entities, improving data quality through automated schema enforcement and row-level quality flagging.
- Built **Azure Synapse Analytics** external tables and SQL views over Parquet-based Gold layer data, enabling self-service BI reporting without duplicating storage.
- Designed and orchestrated a multi-stage **Azure Data Factory pipeline** with parameterized Databricks notebook triggers, conditional branching, and execution auditing/logging for full pipeline observability.
- Authored 20+ production SQL queries and analytical views to surface key business KPIs — total revenue, average order value, top products/customers/sellers, and monthly revenue trends — supporting data-driven decision-making.

---

## License

This project is provided under the MIT License for educational and portfolio purposes. The included dataset is synthetically generated and does not represent real customer, order, or seller data.
