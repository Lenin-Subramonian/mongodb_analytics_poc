#!/usr/bin/env python3
"""
curated_orders_silver.py

Create / refresh curated (silver) orders table from raw Iceberg table.

- Reads:  iceberg.raw.ecommerce_db_orders
- Writes: iceberg.curated.ecommerce_db_orders_silver

Behavior:
- If curated table doesn't exist -> create it (with partitioning)
- For each run:
    * Extract latest records from raw (dedupe by order_id using ingest_ts)
    * Compute flattened/aggregated columns for analytics (total_amount, line_count, distinct_products, etc.)
    * MERGE into curated table (upsert) so reruns are idempotent

Usage:
spark-submit \
  --master local[*] \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.hadoop:hadoop-aws:3.3.6,software.amazon.awssdk:s3:2.20.137,software.amazon.awssdk:sts:2.20.137,software.amazon.awssdk:glue:2.20.137,software.amazon.awssdk:dynamodb:2.20.137,software.amazon.awssdk:kms:2.20.137 \
  src/curated_orders_silver.py
"""

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, current_timestamp, col, explode, sum as spark_sum, countDistinct

logger = logging.getLogger("curated_silver")
logging.basicConfig(level=os.environ.get("LOG_LEVEL","INFO"))

# --- CONFIG: change or provide via env ---
GLUE_CATALOG = os.environ.get("GLUE_CATALOG_NAME", "iceberg")
S3_BUCKET = os.environ.get("S3_BUCKET", "fq-app-analytics-bucket-1")  # replace with actual bucket if not set
WAREHOUSE = os.environ.get("WAREHOUSE", f"s3a://{S3_BUCKET}/iceberg-warehouse/")
WAREHOUSE = os.environ.get("WAREHOUSE", "s3a://fq-app-analytics-bucket-1/iceberg-warehouse/")  # same as ETL
AWS_REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-2"))
RAW_TABLE = os.environ.get("RAW_TABLE", "raw.ecommerce_db_orders")  # catalog-qualified without prefix
CURATED_TABLE = os.environ.get("CURATED_TABLE", "curated.fq_app_orders_silver")

# --- Spark session build (packages still ideally provided via spark-submit --packages) ---
spark = (
    SparkSession.builder.appName("curated-silver")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{GLUE_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{GLUE_CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{GLUE_CATALOG}.warehouse", WAREHOUSE)
    # ensure region visible to AWS SDK v2
    .config("spark.hadoop.fs.s3a.region", AWS_REGION)
    .config("spark.driver.extraJavaOptions", f"-Daws.region={AWS_REGION}")
    .config("spark.executor.extraJavaOptions", f"-Daws.region={AWS_REGION}")
    .getOrCreate()
)

# Make sure we're referencing catalog-qualified identifiers consistently:
RAW_QUALIFIED = f"{GLUE_CATALOG}.{RAW_TABLE}"
CURATED_QUALIFIED = f"{GLUE_CATALOG}.{CURATED_TABLE}"

# -- Helper: create curated namespace if not exists
def ensure_namespace_exists():
    db_name = CURATED_TABLE.split(".", 1)[0]  # e.g. 'curated'
    qualified_db = f"{GLUE_CATALOG}.{db_name}"
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {qualified_db}")
    except Exception as e:
        # fallback to namespace
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {qualified_db}")

# -- Helper: create curated table if not exists with schema suitable for analytics
def create_curated_table_if_missing():
    # We define a schema for silver table. This schema contains flattened fields + metrics.
    # Adjust column types based on your raw schema.
    create_ddl = f"""
    CREATE TABLE IF NOT EXISTS {CURATED_QUALIFIED} (
        order_id STRING,
        customer_id STRING,
        order_date DATE,
        status STRING,
        total_amount DECIMAL(18,2),
        line_count INT,
        distinct_product_count INT,
        first_product_ids ARRAY<STRING>,
        ingest_ts TIMESTAMP,
        last_update_ts TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (days(order_date))
    """
    spark.sql(create_ddl)


# -- Build a DataFrame of "latest raw records per order_id"
def read_latest_raw_orders():
    # read raw table
    raw_df = spark.table(RAW_QUALIFIED)

    # normalize fields:
    # - ensure order_date is date type (raw may have string)
    # - ensure ingest_ts exists
    df = raw_df.withColumn("order_date", to_date(col("order_date"))) \
               .withColumn("ingest_ts", coalesce(col("ingest_ts"), current_timestamp()))

    # Deduplicate: keep the record with the max(ingest_ts) per order_id
    w = Window.partitionBy("order_id").orderBy(col("ingest_ts").desc_nulls_last())
    latest_df = df.withColumn("_row_num", expr("row_number() over (partition by order_id order by ingest_ts desc)")) \
                  .filter(col("_row_num") == 1) \
                  .drop("_row_num")

    return latest_df


# -- Flatten and compute metrics for analytics
def build_flattened_metrics(df_latest):
    # Explode lines to compute per-order aggregates
    # if lines is an array of structs: product_id, quantity, unit_price, line_total
    exploded = df_latest.select(
        "order_id", "customer_id", "order_date", "status", "ingest_ts",
        explode(col("lines")).alias("line")
    ).select(
        col("order_id"),
        col("customer_id"),
        col("order_date"),
        col("status"),
        col("ingest_ts"),
        col("line.product_id").alias("product_id"),
        col("line.quantity").alias("quantity"),
        col("line.unit_price").alias("unit_price"),
        col("line.line_total").alias("line_total")
    )

    # Aggregates per order
    agg = exploded.groupBy("order_id", "customer_id", "order_date", "status", "ingest_ts") \
        .agg(
            spark_sum(col("line_total").cast("decimal(18,2)")).alias("total_amount"),
            spark_count("*").alias("line_count"),
            countDistinct("product_id").alias("distinct_product_count"),
            expr("collect_set(product_id)")[0:5].alias("first_product_ids")  # optional: small set sample
        )

    # Note: above collect_set(...) used for sample; adjust as needed (collect_list) - watch for large arrays.
    # Add last_update_ts for record tracking
    result = agg.withColumn("last_update_ts", col("ingest_ts"))
    return result


# -- Upsert (MERGE) into curated table
def merge_into_curated(df_new):
    # Create temporary view for MERGE
    tmp_view = "tmp_new_orders"
    df_new.createOrReplaceTempView(tmp_view)

    # Ensure table exists
    create_curated_table_if_missing()

    # Use MERGE INTO to upsert: match on order_id
    merge_sql = f"""
    MERGE INTO {CURATED_QUALIFIED} t
    USING {tmp_view} s
    ON t.order_id = s.order_id
    WHEN MATCHED AND s.last_update_ts > t.last_update_ts THEN
      UPDATE SET *
    WHEN NOT MATCHED THEN
      INSERT *
    """
    spark.sql(merge_sql)


def main():
    print("Ensuring curated namespace exists...")
    ensure_namespace_exists()
    print("Reading latest raw orders...")
    latest = read_latest_raw_orders()
    print(f"Latest count: {latest.count()}")
    print("Building flattened metrics...")
    flat = build_flattened_metrics(latest)
    print(f"Flat DF count: {flat.count()}")
    print("Merging into curated table...")
    merge_into_curated(flat)
    print("Curated merge complete.")


if __name__ == "__main__":
    main()
    spark.stop()
