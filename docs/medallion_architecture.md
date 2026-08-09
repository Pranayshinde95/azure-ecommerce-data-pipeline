# Medallion Architecture in This Project

The **medallion architecture** organizes data into three progressively refined layers — Bronze, Silver, and Gold — each with a distinct responsibility. This project implements all three on Azure Data Lake Storage Gen2, processed entirely with PySpark on Azure Databricks.

```
RAW CSV  ──▶  BRONZE  ──▶  SILVER  ──▶  GOLD  ──▶  Synapse Views  ──▶  BI / Reporting
            (as-is +      (cleaned,     (aggregated,
             metadata)     conformed)    business-ready)
```

---

## Bronze Layer — "Raw, but Trustworthy"

**Script:** [`databricks/bronze_layer.py`](../databricks/bronze_layer.py)

**Goal:** Land source data exactly as received, but with enough metadata to trace every row back to its origin.

What happens here:
- Each CSV is read with an **explicit, enforced schema** — if a column is missing, the job fails loudly instead of silently producing nulls.
- `validate_schema()` confirms all required columns exist before any data is written.
- Every row is stamped with `_ingestion_timestamp`, `_ingestion_date`, `_source_file`, `_source_system`, and `_pipeline_run_id` for full lineage.
- Output is written as **partitioned Parquet** (`partitionBy("_ingestion_date")`) to `bronze/<entity>/`.

What does **not** happen here: no deduplication, no null handling, no business logic. Bronze is the immutable system of record for "what we received and when."

---

## Silver Layer — "Clean and Conformed"

**Script:** [`databricks/silver_layer.py`](../databricks/silver_layer.py)

**Goal:** Produce a single, trustworthy version of each entity that downstream consumers can join on without surprises.

What happens here:
- **Deduplication** — `dedupe()` keeps only the most recently ingested row per business key (`customer_id`, `order_id`, etc.) using a window function ranked by `_ingestion_timestamp`.
- **Null handling** — required fields are filtered out if null; optional fields are defaulted (e.g., missing `customer_name` becomes `"Unknown Customer"`).
- **Date standardization** — every date string is parsed into a proper `DateType` via `to_date()`.
- **Category cleaning** — `standardize_category()` lower-cases and snake_cases product categories (`"Home Appliances"` → `home_appliances`).
- **State standardization** — `standardize_state()` upper-cases state codes and maps anything outside the valid Brazilian state list to `"UNKNOWN"`.
- **Quality flagging** — every row gets a `_quality_flag` (`VALID`, `INVALID_STATE`, `INVALID_DATE`, etc.) instead of being silently dropped, so a data-quality review process can inspect flagged rows separately.

Output: `silver/<entity>/` Parquet, ready to be joined and aggregated.

---

## Gold Layer — "Business-Ready"

**Script:** [`databricks/gold_layer.py`](../databricks/gold_layer.py)

**Goal:** Pre-compute the joins and aggregations that business users actually need, so they never have to write a Spark job or a complex SQL join themselves.

Three marts are built, each filtered to **completed orders only** (`delivered`, `shipped`, `invoiced`) so cancelled/returned orders don't inflate revenue figures:

1. **Customer Sales Summary** (`customer_id, total_orders, total_spent, avg_order_value`) — enriched with customer name/city/state for reporting.
2. **Product Performance** (`product_id, product_name, revenue, units_sold`) — built by joining order_items → orders → products and computing `price * quantity` per line.
3. **Seller Performance** (`seller_id, seller_name, revenue, orders_processed`) — same join pattern, grouped by seller instead of product.

A fourth script, [`databricks/business_kpis.py`](../databricks/business_kpis.py), extends Gold with single-purpose KPI tables (Total Revenue, Average Order Value, Top 10 Products, Top 10 Customers, Top Sellers, Monthly Revenue Trend, Repeat Customer Rate) — each written to its own `gold/kpis/<kpi_name>/` path so they can be consumed independently.

---

## Why This Layering Matters

| Without Medallion Architecture | With Medallion Architecture (this project) |
|---|---|
| Every report re-implements its own cleaning logic | Cleaning logic lives once, in Silver |
| A bad source file can silently corrupt a dashboard | Bronze schema validation fails the job before bad data spreads |
| No way to know if a number changed because of a bug or a business change | Bronze is immutable; you can always recompute Silver/Gold from it |
| Analysts write 200-line SQL joins for simple questions | Gold marts and Synapse views answer common questions with a single `SELECT` |
| No accountability for which rows were excluded and why | `_quality_flag` makes exclusions explicit and reviewable |

This is the same layering pattern used in most modern lakehouse implementations (Databricks' own medallion architecture guidance, Microsoft's Cloud Adoption Framework for analytics, and most enterprise data platforms built on ADLS Gen2).
