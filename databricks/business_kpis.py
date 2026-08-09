# Databricks notebook source
"""
business_kpis.py
=================
Layer    : GOLD (KPI / reporting extension)
Purpose  : Calculate the core business KPIs that power executive dashboards
           and the Synapse analytical views: Total Revenue, Average Order
           Value, Top 10 Products, Top 10 Customers, Top Sellers, and Monthly
           Revenue Trend. Results are written to gold/kpis/ as Parquet and
           also printed for quick validation in the Databricks job output.

Triggered: Can run standalone or be chained after Notebook_Gold_Layer in ADF.
Inputs   : abfss://datalake@<storage_account>.dfs.core.windows.net/silver/<entity>/
Outputs  : abfss://datalake@<storage_account>.dfs.core.windows.net/gold/kpis/<kpi_name>/
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("silver_path", "abfss://datalake@ecommercedatalakedev.dfs.core.windows.net/silver")
dbutils.widgets.text("gold_path", "abfss://datalake@ecommercedatalakedev.dfs.core.windows.net/gold")

RUN_DATE = dbutils.widgets.get("run_date")
SILVER_PATH = dbutils.widgets.get("silver_path")
GOLD_PATH = dbutils.widgets.get("gold_path")
KPI_OUTPUT_PATH = f"{GOLD_PATH}/kpis"

spark = SparkSession.builder.appName("Ecommerce_Business_KPIs").getOrCreate()

COMPLETED_STATUSES = ["delivered", "shipped", "invoiced"]


def read_silver(entity_name: str):
    return spark.read.parquet(f"{SILVER_PATH}/{entity_name}").filter(F.col("_quality_flag") == "VALID")


customers_df = read_silver("customers")
orders_df = read_silver("orders")
products_df = read_silver("products")
order_items_df = read_silver("order_items")
sellers_df = read_silver("sellers")

completed_orders_df = orders_df.filter(F.col("order_status").isin(COMPLETED_STATUSES))

# Pre-join order_items -> completed orders -> products once, reuse everywhere below
items_enriched_df = (
    order_items_df
    .join(completed_orders_df.select("order_id", "customer_id", "order_date"), on="order_id", how="inner")
    .join(products_df.select("product_id", "product_name", "category", "price"), on="product_id", how="left")
    .withColumn("line_revenue", F.round(F.col("price") * F.col("quantity"), 2))
)


def save_kpi(df, kpi_name: str):
    out_path = f"{KPI_OUTPUT_PATH}/{kpi_name}"
    df.write.mode("overwrite").parquet(out_path)
    print(f"[KPI SAVED] {kpi_name} -> {out_path}")


# ---------------------------------------------------------------------------
# KPI 1: Total Revenue
# ---------------------------------------------------------------------------
def kpi_total_revenue():
    result = items_enriched_df.agg(F.round(F.sum("line_revenue"), 2).alias("total_revenue"))
    result = result.withColumn("kpi_name", F.lit("total_revenue")).withColumn("run_date", F.lit(RUN_DATE))
    return result


# ---------------------------------------------------------------------------
# KPI 2: Average Order Value
# ---------------------------------------------------------------------------
def kpi_average_order_value():
    order_totals = items_enriched_df.groupBy("order_id").agg(F.sum("line_revenue").alias("order_total"))
    result = order_totals.agg(F.round(F.avg("order_total"), 2).alias("avg_order_value"))
    result = result.withColumn("kpi_name", F.lit("average_order_value")).withColumn("run_date", F.lit(RUN_DATE))
    return result


# ---------------------------------------------------------------------------
# KPI 3: Top 10 Products by Revenue
# ---------------------------------------------------------------------------
def kpi_top_10_products():
    result = (
        items_enriched_df.groupBy("product_id", "product_name", "category")
        .agg(
            F.round(F.sum("line_revenue"), 2).alias("revenue"),
            F.sum("quantity").alias("units_sold"),
        )
        .orderBy(F.col("revenue").desc())
        .limit(10)
        .withColumn("run_date", F.lit(RUN_DATE))
    )
    return result


# ---------------------------------------------------------------------------
# KPI 4: Top 10 Customers by Spend
# ---------------------------------------------------------------------------
def kpi_top_10_customers():
    result = (
        items_enriched_df.groupBy("customer_id")
        .agg(F.round(F.sum("line_revenue"), 2).alias("total_spent"), F.countDistinct("order_id").alias("total_orders"))
        .join(customers_df.select("customer_id", "customer_name", "city", "state"), on="customer_id", how="left")
        .select("customer_id", "customer_name", "city", "state", "total_spent", "total_orders")
        .orderBy(F.col("total_spent").desc())
        .limit(10)
        .withColumn("run_date", F.lit(RUN_DATE))
    )
    return result


# ---------------------------------------------------------------------------
# KPI 5: Top Sellers by Revenue
# ---------------------------------------------------------------------------
def kpi_top_sellers():
    seller_items = order_items_df.join(
        completed_orders_df.select("order_id"), on="order_id", how="inner"
    ).join(products_df.select("product_id", "price"), on="product_id", how="left").withColumn(
        "line_revenue", F.round(F.col("price") * F.col("quantity"), 2)
    )
    result = (
        seller_items.groupBy("seller_id")
        .agg(F.round(F.sum("line_revenue"), 2).alias("revenue"), F.countDistinct("order_id").alias("orders_processed"))
        .join(sellers_df.select("seller_id", "seller_name", "state"), on="seller_id", how="left")
        .select("seller_id", "seller_name", "state", "revenue", "orders_processed")
        .orderBy(F.col("revenue").desc())
        .limit(10)
        .withColumn("run_date", F.lit(RUN_DATE))
    )
    return result


# ---------------------------------------------------------------------------
# KPI 6: Monthly Revenue Trend
# ---------------------------------------------------------------------------
def kpi_monthly_revenue_trend():
    result = (
        items_enriched_df
        .withColumn("order_month", F.date_format(F.col("order_date"), "yyyy-MM"))
        .groupBy("order_month")
        .agg(
            F.round(F.sum("line_revenue"), 2).alias("monthly_revenue"),
            F.countDistinct("order_id").alias("order_count"),
        )
        .orderBy("order_month")
        .withColumn("run_date", F.lit(RUN_DATE))
    )
    return result


# ---------------------------------------------------------------------------
# Bonus KPI: Repeat Customer Rate (commonly requested alongside the above)
# ---------------------------------------------------------------------------
def kpi_repeat_customer_rate():
    order_counts = items_enriched_df.groupBy("customer_id").agg(F.countDistinct("order_id").alias("order_count"))
    total_customers = order_counts.count()
    repeat_customers = order_counts.filter(F.col("order_count") > 1).count()
    rate = round((repeat_customers / total_customers) * 100, 2) if total_customers > 0 else 0.0
    result = spark.createDataFrame(
        [(total_customers, repeat_customers, rate, RUN_DATE)],
        ["total_customers", "repeat_customers", "repeat_customer_rate_pct", "run_date"],
    )
    return result


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 70)
    print(f"BUSINESS KPI CALCULATION STARTED | run_date={RUN_DATE}")
    print("=" * 70)

    kpis = {
        "total_revenue": kpi_total_revenue(),
        "average_order_value": kpi_average_order_value(),
        "top_10_products": kpi_top_10_products(),
        "top_10_customers": kpi_top_10_customers(),
        "top_sellers": kpi_top_sellers(),
        "monthly_revenue_trend": kpi_monthly_revenue_trend(),
        "repeat_customer_rate": kpi_repeat_customer_rate(),
    }

    for kpi_name, kpi_df in kpis.items():
        print(f"\n--- {kpi_name.upper()} ---")
        kpi_df.show(20, truncate=False)
        save_kpi(kpi_df, kpi_name)

    print("\n" + "=" * 70)
    print("BUSINESS KPI CALCULATION COMPLETE")
    print("=" * 70)

    dbutils.notebook.exit(f"KPI_SUCCESS: {len(kpis)} KPI tables written to {KPI_OUTPUT_PATH}")
