# Databricks notebook source
"""
bronze_layer.py
================
Layer    : BRONZE
Purpose  : Raw ingestion of the Brazilian E-Commerce source CSV files into the
           ADLS Gen2 Bronze layer as Parquet, with explicit schema enforcement,
           ingestion metadata, and basic row-count auditing.

Triggered: Azure Data Factory -> Notebook_Bronze_Layer activity
Inputs   : abfss://raw@<storage_account>.dfs.core.windows.net/landing/ecommerce/*.csv
Outputs  : abfss://datalake@<storage_account>.dfs.core.windows.net/bronze/<entity>/
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, DateType
)
from pyspark.sql.utils import AnalysisException
import sys

# ---------------------------------------------------------------------------
# 1. Widgets / parameters (populated by ADF baseParameters at runtime)
# ---------------------------------------------------------------------------
dbutils.widgets.text("run_date", "")
dbutils.widgets.text("input_path", "abfss://raw@ecommercedatalakedev.dfs.core.windows.net/landing/ecommerce")
dbutils.widgets.text("output_path", "abfss://datalake@ecommercedatalakedev.dfs.core.windows.net/bronze")

RUN_DATE = dbutils.widgets.get("run_date")
INPUT_PATH = dbutils.widgets.get("input_path")
OUTPUT_PATH = dbutils.widgets.get("output_path")

spark = SparkSession.builder.appName("Ecommerce_Bronze_Ingestion").getOrCreate()

# ---------------------------------------------------------------------------
# 2. Explicit schemas (fail fast on malformed source data)
# ---------------------------------------------------------------------------
SCHEMAS = {
    "customers": StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("signup_date", StringType(), True),
    ]),
    "orders": StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("payment_value", DoubleType(), True),
    ]),
    "products": StructType([
        StructField("product_id", StringType(), False),
        StructField("category", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("price", DoubleType(), True),
    ]),
    "order_items": StructType([
        StructField("order_item_id", StringType(), False),
        StructField("order_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("seller_id", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("freight_value", DoubleType(), True),
    ]),
    "sellers": StructType([
        StructField("seller_id", StringType(), False),
        StructField("seller_name", StringType(), True),
        StructField("state", StringType(), True),
    ]),
}

REQUIRED_COLUMNS = {
    "customers": {"customer_id", "customer_name", "city", "state", "signup_date"},
    "orders": {"order_id", "customer_id", "order_date", "order_status", "payment_value"},
    "products": {"product_id", "category", "product_name", "price"},
    "order_items": {"order_item_id", "order_id", "product_id", "seller_id", "quantity", "freight_value"},
    "sellers": {"seller_id", "seller_name", "state"},
}


def validate_schema(df, entity_name: str) -> bool:
    """Confirm the incoming dataframe has all required columns before promoting to Bronze."""
    actual_cols = set(df.columns)
    expected_cols = REQUIRED_COLUMNS[entity_name]
    missing = expected_cols - actual_cols
    if missing:
        raise ValueError(
            f"[SCHEMA VALIDATION FAILED] Entity '{entity_name}' is missing required columns: {missing}"
        )
    print(f"[SCHEMA OK] '{entity_name}' contains all {len(expected_cols)} required columns.")
    return True


def read_raw_csv(entity_name: str):
    """Read a single raw CSV file with its enforced schema."""
    file_path = f"{INPUT_PATH}/{entity_name}.csv"
    print(f"Reading raw file: {file_path}")
    df = (
        spark.read
        .option("header", "true")
        .option("escape", '"')
        .schema(SCHEMAS[entity_name])
        .csv(file_path)
    )
    return df


def add_ingestion_metadata(df, entity_name: str, source_file: str):
    """Stamp every Bronze record with lineage / ingestion metadata for traceability."""
    return (
        df
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_ingestion_date", F.coalesce(F.lit(RUN_DATE), F.current_date().cast(StringType())))
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_source_system", F.lit("ecommerce_landing_zone"))
        .withColumn("_pipeline_run_id", F.lit(spark.conf.get("spark.databricks.job.runId", "manual_run")))
        .withColumn("_layer", F.lit("bronze"))
    )


def write_bronze(df, entity_name: str):
    """Write the enriched Bronze dataframe out as partitioned Parquet."""
    target_path = f"{OUTPUT_PATH}/{entity_name}"
    print(f"Writing Bronze output -> {target_path}")
    (
        df.write
        .mode("overwrite")
        .option("mergeSchema", "true")
        .partitionBy("_ingestion_date")
        .parquet(target_path)
    )
    row_count = df.count()
    print(f"[BRONZE WRITE COMPLETE] entity='{entity_name}' rows={row_count} path={target_path}")
    return row_count


def process_entity(entity_name: str):
    """End-to-end Bronze processing for a single entity: read -> validate -> enrich -> write."""
    try:
        raw_df = read_raw_csv(entity_name)
        validate_schema(raw_df, entity_name)
        enriched_df = add_ingestion_metadata(raw_df, entity_name, f"{entity_name}.csv")
        row_count = write_bronze(enriched_df, entity_name)
        return {"entity": entity_name, "status": "SUCCESS", "rows": row_count}
    except AnalysisException as e:
        print(f"[ERROR] Failed to read/process '{entity_name}': {str(e)}")
        return {"entity": entity_name, "status": "FAILED", "error": str(e)}
    except Exception as e:
        print(f"[ERROR] Unexpected failure on '{entity_name}': {str(e)}")
        return {"entity": entity_name, "status": "FAILED", "error": str(e)}


# ---------------------------------------------------------------------------
# 3. Main execution
# ---------------------------------------------------------------------------
ENTITIES = ["customers", "orders", "products", "order_items", "sellers"]

if __name__ == "__main__":
    print("=" * 70)
    print(f"BRONZE LAYER INGESTION STARTED | run_date={RUN_DATE}")
    print("=" * 70)

    results = [process_entity(entity) for entity in ENTITIES]

    failures = [r for r in results if r["status"] == "FAILED"]
    successes = [r for r in results if r["status"] == "SUCCESS"]

    print("\n" + "=" * 70)
    print("BRONZE LAYER SUMMARY")
    print("=" * 70)
    for r in results:
        print(r)

    total_rows = sum(r.get("rows", 0) for r in successes)
    print(f"\nTotal entities processed : {len(results)}")
    print(f"Successful               : {len(successes)}")
    print(f"Failed                   : {len(failures)}")
    print(f"Total rows ingested      : {total_rows}")

    if failures:
        # Fail the Databricks job so ADF marks the activity as Failed and triggers
        # the Log_Pipeline_Failure branch of the orchestrating pipeline.
        dbutils.notebook.exit(f"BRONZE_FAILED: {failures}")
        sys.exit(1)
    else:
        dbutils.notebook.exit(f"BRONZE_SUCCESS: {total_rows} rows ingested across {len(successes)} entities")
