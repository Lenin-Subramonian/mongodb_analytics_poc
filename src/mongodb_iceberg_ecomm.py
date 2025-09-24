#!/usr/bin/env python3
"""
mongodb_to_iceberg_etl.py

POC ETL: extract collections from MongoDB (using pymongo) and write into Iceberg tables
backed by AWS Glue Catalog. Uses Spark 3.5.x DataFrameWriterV2 (writeTo API).

Notes:
- This script streams documents in batches from MongoDB to avoid loading everything into memory.
- For production/large scale, use the Mongo Spark Connector or change stream approach.
"""

import os
import sys
import json
import decimal
from typing import Dict, Any, Iterable, List

from pymongo import MongoClient
from bson import ObjectId
from datetime import datetime, date

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, TimestampType,
    ArrayType, IntegerType, DoubleType, DecimalType, LongType, BooleanType, MapType
)
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import current_timestamp

mongo_atlas_uri = "mongodb+srv://ls_db_user:s47tBnWikllAoe3k@democluster0.j777gry.mongodb.net/"
# S3_BUCKET = "s3://fq-app-analytics-bucket-1/iceberg-warehouse/ecommerce_db_raw/"

# -----------------------------------------------------------------------------
# Configuration (via ENV or defaults)
# -----------------------------------------------------------------------------
MONGO_URI = os.environ.get("MONGO_URI", mongo_atlas_uri)
MONGO_DB = os.environ.get("MONGO_DB", "ecommerce_db")
COLLECTIONS = ["orders"]
# COLLECTIONS = ["accounts", "close_tasks", "journal_entries"]
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))

# Iceberg / Glue / S3 config
GLUE_CATALOG_NAME = os.environ.get("GLUE_CATALOG_NAME", "iceberg")  # logical name used in spark configs
S3_BUCKET = os.environ.get("S3_BUCKET", "fq-app-analytics-bucket-1")  # replace with actual bucket if not set
WAREHOUSE = os.environ.get("WAREHOUSE", f"s3a://{S3_BUCKET}/iceberg-warehouse/")

# Spark packages should be provided on spark-submit --packages in docker-compose
# e.g. iceberg-spark-runtime, hadoop-aws, aws-java-sdk-bundle

# -----------------------------------------------------------------------------
# Helper: conversion of mongo document (bson types) -> JSON-friendly dict for Spark
# -----------------------------------------------------------------------------
def normalize_bson(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert BSON types (ObjectId, datetime, Decimal128, etc.) to Python types that Spark accepts.
    Keeps nested structures intact.
    """
    def conv(value):
        if isinstance(value, ObjectId):
            return str(value)
        if isinstance(value, (datetime, )):
            # keep as ISO string or datetime object; Spark will infer or schema will cast
            return value.isoformat()
        if isinstance(value, date) and not isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, decimal.Decimal):
            # convert Decimal to float or string; we prefer string then cast with DecimalType if needed
            # but to preserve precision we return string and let Spark cast later if schema uses DecimalType.
            return str(value)
        if isinstance(value, dict):
            return {k: conv(v) for k, v in value.items()}
        if isinstance(value, list):
            return [conv(v) for v in value]
        # default types (int, float, str, bool, None) are acceptable
        return value

    return {k: conv(v) for k, v in doc.items()}


# -----------------------------------------------------------------------------
# Define schemas for the collections (example). Adjust as necessary for your data.
# Use DecimalType for money fields if needed (recommended).
# -----------------------------------------------------------------------------
money_type = DecimalType(18, 2)

orders_schema = StructType([
    StructField("_id", StringType(), nullable=False),
    StructField("order_id", StringType(), nullable=True),
    StructField("customer_id", StringType(), nullable=True),
    StructField("order_date", StringType(), nullable=True),  # store ISO string or convert to date later
    StructField("status", StringType(), nullable=True),
    StructField("total_amount", StringType(), nullable=True),  # string to allow decimal preservation; cast later
    StructField("lines", ArrayType(
        StructType([
            StructField("product_id", StringType(), nullable=True),
            StructField("quantity", IntegerType(), nullable=True),
            StructField("unit_price", StringType(), nullable=True),  # string for decimal preservation
            StructField("line_total", StringType(), nullable=True)
        ])
    ), nullable=True),
    StructField("ingest_ts", TimestampType(), nullable=True)
])

customers_schema = StructType([
    StructField("_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("email", StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("ingest_ts", TimestampType(), nullable=True)
])

products_schema = StructType([
    StructField("_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("category", StringType(), nullable=True),
    StructField("price", StringType(), nullable=True),
    StructField("ingest_ts", TimestampType(), nullable=True)
])


SCHEMAS = {
    "orders": (orders_schema, "raw.ecommerce_db_orders"),
    "customers": (customers_schema, "raw.ecommerce_db_customers"),
    "products": (products_schema, "raw.ecommerce_db_products"),
}


# -----------------------------------------------------------------------------
# Spark session builder with GlueCatalog (Iceberg) configuration
# NOTE: pass needed packages via spark-submit --packages (see run section above)
# -----------------------------------------------------------------------------
def create_spark_session(app_name="mongo-to-iceberg-etl"):
    builder = SparkSession.builder.appName(app_name)
    # icebergs configs should be passed as spark-submit --conf too, but we set helpful defaults:
    builder = builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{GLUE_CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{GLUE_CATALOG_NAME}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config(f"spark.sql.catalog.{GLUE_CATALOG_NAME}.warehouse", WAREHOUSE) \
        .config("spark.sql.catalogImplementation", "hive")  # ensure Hive-style catalog support
    return builder.getOrCreate()

    # spark = SparkSession.builder \
    # .appName("etl") \
    # .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    # .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    # .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    # .config("spark.sql.catalog.iceberg.warehouse", "s3a://fq-app-analytics-bucket-1/iceberg-warehouse/") \
    # .getOrCreate()


# -----------------------------------------------------------------------------
# Helper: check if table exists in the glue catalog
# -----------------------------------------------------------------------------
def table_exists(spark: SparkSession, full_table_name: str) -> bool:
    # full_table_name like "raw.ecommerce_db_orders" (without catalog prefix)
    qualified = f"{GLUE_CATALOG_NAME}.{full_table_name}"
    try:
        spark.table(qualified)
        return True
    except AnalysisException:
        return False


# -----------------------------------------------------------------------------
# Core: ingest collection in batches using pymongo cursor
# -----------------------------------------------------------------------------
def ingest_collection_mongo(
    spark: SparkSession,
    mongo_uri: str,
    db_name: str,
    coll_name: str,
    schema,
    table_name_full: str,
    batch_size: int = 1000
):
    """
    Stream documents from MongoDB (pymongo) in batches and write each batch into Iceberg table.
    - If table doesn't exist: the first non-empty batch will create the table via writer.create()
    - Subsequent batches use writer.append()
    """
    client = MongoClient(mongo_uri)
    db = client[db_name]
    coll = db[coll_name]

    # sort or filter can be applied here for incremental ingestion (e.g., updated_at)
    cursor = coll.find({}, batch_size=batch_size)

    spark_table_qualified = f"{GLUE_CATALOG_NAME}.{table_name_full}"

    table_created = table_exists(spark, table_name_full)
    total = 0
    batch_num = 0

    docs_buffer: List[Dict[str, Any]] = []

    for doc in cursor:
        docs_buffer.append(normalize_bson(doc))
        if len(docs_buffer) >= batch_size:
            batch_num += 1
            print(f"[{coll_name}] writing batch #{batch_num} with {len(docs_buffer)} docs")
            write_batch_to_iceberg(spark, docs_buffer, schema, spark_table_qualified, table_created)
            table_created = True
            total += len(docs_buffer)
            docs_buffer = []

    # final partial batch
    if docs_buffer:
        batch_num += 1
        print(f"[{coll_name}] writing final batch #{batch_num} with {len(docs_buffer)} docs")
        write_batch_to_iceberg(spark, docs_buffer, schema, spark_table_qualified, table_created)
        total += len(docs_buffer)

    print(f"[{coll_name}] ingestion complete: total rows written = {total}")
    client.close()


def write_batch_to_iceberg(spark: SparkSession, docs_batch: List[Dict[str, Any]],
                           schema: StructType, qualified_table_name: str, table_created: bool):
    """
    Convert a python list of normalized dicts into a Spark DF using the provided schema,
    add ingest_ts, then create or append to Iceberg table.
    """
    if not docs_batch:
        return

    # Create DataFrame from list of dicts using predefined schema to enforce types/columns
    df = spark.createDataFrame(docs_batch, schema=schema)

    # add ingest timestamp
    df = df.withColumn("ingest_ts", current_timestamp())

    # on first write, create() will create the table and write the batch atomically;
    # on subsequent batches append() will append
    if not table_created:
        print(f"Creating table {qualified_table_name} and writing initial batch (rows={df.count()})")
        # DataFrameWriterV2 create will create the Iceberg table and write the data
        df.writeTo(qualified_table_name).create()
    else:
        print(f"Appending to table {qualified_table_name} (rows={df.count()})")
        df.writeTo(qualified_table_name).append()


# -----------------------------------------------------------------------------
# Main entry: orchestrate extraction for desired collections
# -----------------------------------------------------------------------------
def main():
    # Basic checks
    if S3_BUCKET == "fq-app-analytics-bucket-1":
        print("WARNING: S3_BUCKET is not set. Set env var S3_BUCKET to your bucket and re-run.")
    print(f"Using Mongo URI: {MONGO_URI}")
    print(f"Using Glue catalog: {GLUE_CATALOG_NAME}, warehouse: {WAREHOUSE}")

    spark = create_spark_session()

    # Ensure database (namespace) exists in Glue Catalog
    db_namespace = "raw"
    qualified_db = f"{GLUE_CATALOG_NAME}.{db_namespace}"
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {qualified_db}")
    except Exception as e:
        # Some GlueCatalog builds accept CREATE DATABASE as `CREATE NAMESPACE`
        print("Warning when creating database - attempting alternative statement:", str(e))
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {qualified_db}")

    # Ingest collections
    for coll in COLLECTIONS:
        if coll not in SCHEMAS:
            print(f"Skipping unknown collection {coll}")
            continue

        schema, table_suffix = SCHEMAS[coll]
        # table name in glue_catalog should be like raw.ecommerce_db_orders
        namespace_table = table_suffix  # includes "raw.<table>"
        print(f"Starting ingestion for collection: {coll} -> {GLUE_CATALOG_NAME}.{namespace_table}")
        try:
            ingest_collection_mongo(
                spark,
                MONGO_URI,
                MONGO_DB,
                coll,
                schema,
                namespace_table,
                batch_size=BATCH_SIZE
            )
        except Exception as ex:
            print(f"ERROR ingesting collection {coll}: {ex}", file=sys.stderr)

    spark.stop()
    print("ETL job finished.")


if __name__ == "__main__":
    main()
