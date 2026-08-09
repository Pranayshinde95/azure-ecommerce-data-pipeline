# Databricks notebook source
"""
transformations.py
===================
Shared, reusable PySpark transformation utilities used across the Bronze,
Silver, and Gold notebooks. Centralizing these functions keeps the
medallion-layer notebooks thin and makes the cleaning/enrichment logic
unit-testable in isolation.

Import this module from other notebooks with:
    %run ./transformations
or, in a packaged repo, with:
    from databricks.transformations import *
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType


# ---------------------------------------------------------------------------
# Generic data-quality helpers
# ---------------------------------------------------------------------------
def drop_exact_duplicates(df: DataFrame) -> DataFrame:
    """Drop fully duplicated rows (all columns identical)."""
    before = df.count()
    deduped = df.dropDuplicates()
    after = deduped.count()
    print(f"[drop_exact_duplicates] removed {before - after} fully duplicate rows")
    return deduped


def dedupe_by_key(df: DataFrame, key_cols: list, order_col: str, descending: bool = True) -> DataFrame:
    """Keep one row per business key, preferring the most recent record by order_col."""
    order_expr = F.col(order_col).desc() if descending else F.col(order_col).asc()
    w = Window.partitionBy(*key_cols).orderBy(order_expr)
    return (
        df.withColumn("_dedupe_rank", F.row_number().over(w))
        .filter(F.col("_dedupe_rank") == 1)
        .drop("_dedupe_rank")
    )


def fill_nulls(df: DataFrame, fill_map: dict) -> DataFrame:
    """Fill nulls per-column using a {column_name: fill_value} mapping."""
    return df.fillna(fill_map)


def cast_columns(df: DataFrame, type_map: dict) -> DataFrame:
    """Cast a set of columns to target Spark types, e.g. {'price': DoubleType()}."""
    for col_name, target_type in type_map.items():
        df = df.withColumn(col_name, F.col(col_name).cast(target_type))
    return df


def trim_string_columns(df: DataFrame, columns: list) -> DataFrame:
    """Trim whitespace on the given string columns."""
    for c in columns:
        df = df.withColumn(c, F.trim(F.col(c)))
    return df


def remove_negative_values(df: DataFrame, columns: list) -> DataFrame:
    """Filter out rows where any of the given numeric columns is negative."""
    condition = None
    for c in columns:
        clause = F.col(c) >= 0
        condition = clause if condition is None else (condition & clause)
    return df.filter(condition) if condition is not None else df


# ---------------------------------------------------------------------------
# Date / text standardization helpers
# ---------------------------------------------------------------------------
def standardize_date_column(df: DataFrame, col_name: str, source_format: str = "yyyy-MM-dd") -> DataFrame:
    """Parse a free-text date column into a proper DateType, defaulting to ISO format."""
    return df.withColumn(col_name, F.to_date(F.col(col_name), source_format))


def normalize_text(df: DataFrame, col_name: str, mode: str = "lower") -> DataFrame:
    """Normalize text casing. mode is one of: 'lower', 'upper', 'title'."""
    if mode == "lower":
        return df.withColumn(col_name, F.lower(F.trim(F.col(col_name))))
    if mode == "upper":
        return df.withColumn(col_name, F.upper(F.trim(F.col(col_name))))
    if mode == "title":
        return df.withColumn(col_name, F.initcap(F.trim(F.col(col_name))))
    raise ValueError(f"Unsupported normalize mode: {mode}")


def slugify_column(df: DataFrame, col_name: str) -> DataFrame:
    """Convert free text into a snake_case slug, e.g. 'Home Appliances' -> 'home_appliances'."""
    slugged = F.lower(F.trim(F.regexp_replace(F.regexp_replace(F.col(col_name), "[- ]+", "_"), "[^a-zA-Z0-9_]", "")))
    return df.withColumn(col_name, slugged)


# ---------------------------------------------------------------------------
# Enrichment / lineage helpers
# ---------------------------------------------------------------------------
def add_audit_columns(df: DataFrame, layer: str, source_file: str = None) -> DataFrame:
    """Attach standard audit/lineage columns used across every medallion layer."""
    out = df.withColumn(f"_{layer}_processed_at", F.current_timestamp()).withColumn("_layer", F.lit(layer))
    if source_file:
        out = out.withColumn("_source_file", F.lit(source_file))
    return out


def flag_quality_issues(df: DataFrame, rules: dict) -> DataFrame:
    """
    Apply a dict of {flag_name: spark_condition} rules and attach a single
    `_quality_flag` column = the first rule name whose condition is True,
    or 'VALID' if none match.
    """
    expr = F.lit("VALID")
    for flag_name, condition in reversed(list(rules.items())):
        expr = F.when(condition, F.lit(flag_name)).otherwise(expr)
    return df.withColumn("_quality_flag", expr)


# ---------------------------------------------------------------------------
# Aggregation helpers reused by Gold + business_kpis
# ---------------------------------------------------------------------------
def revenue_by_dimension(items_df: DataFrame, orders_df: DataFrame, products_df: DataFrame,
                          dimension_col: str, completed_statuses: list) -> DataFrame:
    """
    Generic revenue rollup: join order_items -> orders (filtered to completed
    statuses) -> products, compute line revenue, and group by an arbitrary
    dimension column (e.g. category, seller_id, customer_id via order join).
    """
    valid_orders = orders_df.filter(F.col("order_status").isin(completed_statuses))
    joined = (
        items_df.join(valid_orders.select("order_id"), on="order_id", how="inner")
        .join(products_df.select("product_id", "price", dimension_col) if dimension_col in products_df.columns
              else products_df.select("product_id", "price"), on="product_id", how="left")
    )
    joined = joined.withColumn("line_revenue", F.round(F.col("price") * F.col("quantity"), 2))
    return (
        joined.groupBy(dimension_col)
        .agg(F.round(F.sum("line_revenue"), 2).alias("revenue"))
        .orderBy(F.col("revenue").desc())
    )


def top_n(df: DataFrame, order_col: str, n: int = 10, descending: bool = True) -> DataFrame:
    """Return the top-N rows of a dataframe ordered by a given column."""
    order_expr = F.col(order_col).desc() if descending else F.col(order_col).asc()
    return df.orderBy(order_expr).limit(n)
