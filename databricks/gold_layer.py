# Databricks notebook source
"""
gold_layer.py
=============
Layer    : GOLD
Purpose  : Build business-ready, aggregated Gold tables on top of the cleaned
           Silver layer: Customer Sales Summary, Product Performance, and
           Seller Performance. These tables back the Synapse external tables
           and Power BI / analytical reporting layer.

Triggered: Azure Data Factory -> Notebook_Gold_Layer activity
Inputs   : abfss://datalake@<storage_account>.dfs.core.windows.net/silver/<entity>/
Outputs  : abfss://datalake@<storage_account>.dfs.core.windows.net/gold/<mart>/
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("silver_path", "abfss://datalake@ecommercedatalakedev.dfs.core.windows.net/silver")
dbutils.widgets.text("gold_path", "abfss://datalake@ecommercedatalakedev.dfs.core.windows.net/gold")

RUN_DATE = dbutils.widgets.get("run_date")
SILVER_PATH = dbutils.widgets.get("silver_path")
GOLD_PATH = dbutils.widgets.get("gold_path")

spark = SparkSession.builder.appName("Ecommerce_Gold_Aggregation").getOrCreate()


def read_silver(entity_name: str):
    path = f"{SILVER_PATH}/{entity_name}"
    print(f"Reading Silver entity '{entity_name}' from {path}")
    return spark.read.parquet(path)


def write_gold(df, mart_name: str):
    target_path = f"{GOLD_PATH}/{mart_name}"
    (
        df.write
        .mode("overwrite")
        .option("mergeSchema", "true")
        .parquet(target_path)
    )
    row_count = df.count()
    print(f"[GOLD WRITE COMPLETE] mart='{mart_name}' rows={row_count} path={target_path}")
    return row_count


# ---------------------------------------------------------------------------
# Load Silver inputs once and reuse across marts
# ---------------------------------------------------------------------------
customers_df = read_silver("customers").filter(F.col("_quality_flag") == "VALID")
orders_df = read_silver("orders").filter(F.col("_quality_flag") == "VALID")
products_df = read_silver("products").filter(F.col("_quality_flag") == "VALID")
order_items_df = read_silver("order_items").filter(F.col("_quality_flag") == "VALID")
sellers_df = read_silver("sellers").filter(F.col("_quality_flag") == "VALID")

# Only consider orders that were actually fulfilled for revenue-bearing marts
COMPLETED_STATUSES = ["delivered", "shipped", "invoiced"]


# ---------------------------------------------------------------------------
# 1. Customer Sales Summary
#    columns: customer_id, total_orders, total_spent, avg_order_value
# ---------------------------------------------------------------------------
def build_customer_sales_summary():
    valid_orders = orders_df.filter(F.col("order_status").isin(COMPLETED_STATUSES))

    summary = (
        valid_orders.groupBy("customer_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("payment_value"), 2).alias("total_spent"),
        )
        .withColumn(
            "avg_order_value",
            F.round(F.col("total_spent") / F.col("total_orders"), 2),
        )
    )

    # Enrich with customer attributes for easier downstream reporting
    enriched = (
        summary.join(
            customers_df.select("customer_id", "customer_name", "city", "state"),
            on="customer_id",
            how="left",
        )
        .select(
            "customer_id",
            "customer_name",
            "city",
            "state",
            "total_orders",
            "total_spent",
            "avg_order_value",
        )
        .withColumn("_gold_generated_at", F.current_timestamp())
        .orderBy(F.col("total_spent").desc())
    )
    return enriched


# ---------------------------------------------------------------------------
# 2. Product Performance
#    columns: product_id, product_name, revenue, units_sold
# ---------------------------------------------------------------------------
def build_product_performance():
    items_with_orders = order_items_df.join(
        orders_df.filter(F.col("order_status").isin(COMPLETED_STATUSES)).select(
            "order_id", "order_status"
        ),
        on="order_id",
        how="inner",
    )

    items_with_price = items_with_orders.join(
        products_df.select("product_id", "product_name", "category", "price"),
        on="product_id",
        how="left",
    ).withColumn("line_revenue", F.round(F.col("price") * F.col("quantity"), 2))

    summary = (
        items_with_price.groupBy("product_id", "product_name", "category")
        .agg(
            F.round(F.sum("line_revenue"), 2).alias("revenue"),
            F.sum("quantity").alias("units_sold"),
        )
        .withColumn("_gold_generated_at", F.current_timestamp())
        .orderBy(F.col("revenue").desc())
    )
    return summary


# ---------------------------------------------------------------------------
# 3. Seller Performance
#    columns: seller_id, seller_name, revenue, orders_processed
# ---------------------------------------------------------------------------
def build_seller_performance():
    items_with_orders = order_items_df.join(
        orders_df.filter(F.col("order_status").isin(COMPLETED_STATUSES)).select(
            "order_id", "order_status"
        ),
        on="order_id",
        how="inner",
    )

    items_with_price = items_with_orders.join(
        products_df.select("product_id", "price"), on="product_id", how="left"
    ).withColumn("line_revenue", F.round(F.col("price") * F.col("quantity"), 2))

    summary = (
        items_with_price.groupBy("seller_id")
        .agg(
            F.round(F.sum("line_revenue"), 2).alias("revenue"),
            F.countDistinct("order_id").alias("orders_processed"),
        )
        .join(sellers_df.select("seller_id", "seller_name", "state"), on="seller_id", how="left")
        .select("seller_id", "seller_name", "state", "revenue", "orders_processed")
        .withColumn("_gold_generated_at", F.current_timestamp())
        .orderBy(F.col("revenue").desc())
    )
    return summary


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 70)
    print(f"GOLD LAYER AGGREGATION STARTED | run_date={RUN_DATE}")
    print("=" * 70)

    customer_sales_summary = build_customer_sales_summary()
    customer_rows = write_gold(customer_sales_summary, "customer_sales_summary")

    product_performance = build_product_performance()
    product_rows = write_gold(product_performance, "product_performance")

    seller_performance = build_seller_performance()
    seller_rows = write_gold(seller_performance, "seller_performance")

    print("\n" + "=" * 70)
    print("GOLD LAYER SUMMARY")
    print("=" * 70)
    print(f"customer_sales_summary : {customer_rows} rows")
    print(f"product_performance    : {product_rows} rows")
    print(f"seller_performance     : {seller_rows} rows")

    dbutils.notebook.exit(
        f"GOLD_SUCCESS: customers={customer_rows}, products={product_rows}, sellers={seller_rows}"
    )
