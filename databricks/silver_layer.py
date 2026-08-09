# Databricks notebook source
"""
silver_layer.py
================
Layer    : SILVER
Purpose  : Clean and conform the Bronze layer data: remove duplicates, handle
           nulls, standardize date formats, normalize category names and
           Brazilian state codes, and enforce data quality rules.

Triggered: Azure Data Factory -> Notebook_Silver_Layer activity
Inputs   : abfss://datalake@<storage_account>.dfs.core.windows.net/bronze/<entity>/
Outputs  : abfss://datalake@<storage_account>.dfs.core.windows.net/silver/<entity>/
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

dbutils.widgets.text("run_date", "")
dbutils.widgets.text("bronze_path", "abfss://datalake@ecommercedatalakedev.dfs.core.windows.net/bronze")
dbutils.widgets.text("silver_path", "abfss://datalake@ecommercedatalakedev.dfs.core.windows.net/silver")

RUN_DATE = dbutils.widgets.get("run_date")
BRONZE_PATH = dbutils.widgets.get("bronze_path")
SILVER_PATH = dbutils.widgets.get("silver_path")

spark = SparkSession.builder.appName("Ecommerce_Silver_Transformation").getOrCreate()

# ---------------------------------------------------------------------------
# Reference data for standardization
# ---------------------------------------------------------------------------
VALID_BR_STATES = {
    "SP", "RJ", "MG", "RS", "PR", "SC", "BA", "GO", "PE", "CE", "DF", "ES",
    "PA", "MT", "MS", "AM", "MA", "PB", "RN", "AL", "PI", "SE", "TO", "AC",
    "RO", "RR", "AP",
}

VALID_ORDER_STATUSES = {
    "delivered", "shipped", "processing", "canceled", "invoiced", "returned",
}


def read_bronze(entity_name: str):
    path = f"{BRONZE_PATH}/{entity_name}"
    print(f"Reading Bronze entity '{entity_name}' from {path}")
    return spark.read.parquet(path)


def standardize_state(col):
    """Upper-case and trim state codes; map obviously invalid codes to 'UNKNOWN'."""
    cleaned = F.upper(F.trim(col))
    return F.when(cleaned.isin(list(VALID_BR_STATES)), cleaned).otherwise(F.lit("UNKNOWN"))


def standardize_category(col):
    """Lower-case, trim, and replace spaces/hyphens with underscores for category names."""
    return F.lower(F.trim(F.regexp_replace(F.regexp_replace(col, "[- ]+", "_"), "[^a-zA-Z0-9_]", "")))


def standardize_date(col, fmt="yyyy-MM-dd"):
    """Parse a string column into a proper DateType using the canonical ISO format."""
    return F.to_date(col, fmt)


def dedupe(df, key_cols, order_col="_ingestion_timestamp"):
    """Keep only the most recently ingested record per business key."""
    w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return (
        df.withColumn("_row_num", F.row_number().over(w))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num")
    )


# ---------------------------------------------------------------------------
# Entity-specific cleaning logic
# ---------------------------------------------------------------------------
def clean_customers():
    df = read_bronze("customers")
    df = dedupe(df, ["customer_id"])
    df = (
        df
        .filter(F.col("customer_id").isNotNull())
        .withColumn("customer_name", F.trim(F.initcap(F.col("customer_name"))))
        .withColumn("city", F.trim(F.initcap(F.col("city"))))
        .withColumn("state", standardize_state(F.col("state")))
        .withColumn("signup_date", standardize_date(F.col("signup_date")))
        .withColumn("customer_name", F.coalesce(F.col("customer_name"), F.lit("Unknown Customer")))
        .withColumn("_quality_flag",
                    F.when(F.col("state") == "UNKNOWN", "INVALID_STATE")
                     .when(F.col("signup_date").isNull(), "INVALID_DATE")
                     .otherwise("VALID"))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )
    return df


def clean_orders():
    df = read_bronze("orders")
    df = dedupe(df, ["order_id"])
    df = (
        df
        .filter(F.col("order_id").isNotNull() & F.col("customer_id").isNotNull())
        .withColumn("order_date", standardize_date(F.col("order_date")))
        .withColumn("order_status", F.lower(F.trim(F.col("order_status"))))
        .withColumn(
            "order_status",
            F.when(F.col("order_status").isin(list(VALID_ORDER_STATUSES)), F.col("order_status"))
             .otherwise(F.lit("unknown")),
        )
        .withColumn("payment_value", F.coalesce(F.col("payment_value"), F.lit(0.0)))
        .withColumn("payment_value", F.round(F.col("payment_value"), 2))
        .filter(F.col("payment_value") >= 0)
        .withColumn("_quality_flag",
                    F.when(F.col("order_date").isNull(), "INVALID_DATE")
                     .when(F.col("order_status") == "unknown", "INVALID_STATUS")
                     .otherwise("VALID"))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )
    return df


def clean_products():
    df = read_bronze("products")
    df = dedupe(df, ["product_id"])
    df = (
        df
        .filter(F.col("product_id").isNotNull())
        .withColumn("category", standardize_category(F.col("category")))
        .withColumn("product_name", F.trim(F.col("product_name")))
        .withColumn("price", F.coalesce(F.col("price"), F.lit(0.0)))
        .withColumn("price", F.round(F.col("price"), 2))
        .filter(F.col("price") > 0)
        .withColumn("_quality_flag",
                    F.when(F.col("category") == "", "MISSING_CATEGORY")
                     .otherwise("VALID"))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )
    return df


def clean_order_items():
    df = read_bronze("order_items")
    df = dedupe(df, ["order_item_id"])
    df = (
        df
        .filter(
            F.col("order_item_id").isNotNull()
            & F.col("order_id").isNotNull()
            & F.col("product_id").isNotNull()
        )
        .withColumn("quantity", F.coalesce(F.col("quantity"), F.lit(1)))
        .filter(F.col("quantity") > 0)
        .withColumn("freight_value", F.coalesce(F.col("freight_value"), F.lit(0.0)))
        .withColumn("freight_value", F.round(F.col("freight_value"), 2))
        .filter(F.col("freight_value") >= 0)
        .withColumn("_quality_flag", F.lit("VALID"))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )
    return df


def clean_sellers():
    df = read_bronze("sellers")
    df = dedupe(df, ["seller_id"])
    df = (
        df
        .filter(F.col("seller_id").isNotNull())
        .withColumn("seller_name", F.trim(F.col("seller_name")))
        .withColumn("state", standardize_state(F.col("state")))
        .withColumn("_quality_flag",
                    F.when(F.col("state") == "UNKNOWN", "INVALID_STATE").otherwise("VALID"))
        .withColumn("_silver_processed_at", F.current_timestamp())
    )
    return df


def write_silver(df, entity_name: str):
    target_path = f"{SILVER_PATH}/{entity_name}"
    (
        df.write
        .mode("overwrite")
        .option("mergeSchema", "true")
        .parquet(target_path)
    )
    row_count = df.count()
    invalid_count = df.filter(F.col("_quality_flag") != "VALID").count()
    print(f"[SILVER WRITE COMPLETE] entity='{entity_name}' rows={row_count} "
          f"flagged_invalid={invalid_count} path={target_path}")
    return row_count, invalid_count


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------
CLEANING_FUNCS = {
    "customers": clean_customers,
    "orders": clean_orders,
    "products": clean_products,
    "order_items": clean_order_items,
    "sellers": clean_sellers,
}

if __name__ == "__main__":
    print("=" * 70)
    print(f"SILVER LAYER TRANSFORMATION STARTED | run_date={RUN_DATE}")
    print("=" * 70)

    summary = []
    for entity_name, clean_fn in CLEANING_FUNCS.items():
        print(f"\nProcessing entity: {entity_name}")
        cleaned_df = clean_fn()
        rows, invalid = write_silver(cleaned_df, entity_name)
        summary.append({"entity": entity_name, "rows": rows, "flagged_invalid": invalid})

    print("\n" + "=" * 70)
    print("SILVER LAYER SUMMARY")
    print("=" * 70)
    for s in summary:
        print(s)

    total_rows = sum(s["rows"] for s in summary)
    total_invalid = sum(s["flagged_invalid"] for s in summary)
    print(f"\nTotal Silver rows written : {total_rows}")
    print(f"Total flagged for review  : {total_invalid}")

    dbutils.notebook.exit(f"SILVER_SUCCESS: {total_rows} rows across {len(summary)} entities")
