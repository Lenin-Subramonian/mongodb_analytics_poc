#!/usr/bin/env python3
import os
import sys
import time
import logging
import json
import decimal
from typing import Dict, Any, Iterable, List
from bson import ObjectId
from datetime import datetime, date
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, TimestampType, ArrayType, IntegerType, DecimalType, BooleanType)
from pyspark.sql.utils import AnalysisException


logger = logging.getLogger("mongodb_iceberg_fqapp.py.py")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

"""
src\mongodb_iceberg_fqapp.py
POC ETL: extract collections from MongoDB (using pymongo) and write into Iceberg tables
backed by AWS Glue Catalog. Uses Spark 3.5.x DataFrameWriterV2 (writeTo API).

- This script streams documents in batches from MongoDB to avoid loading everything into memory.
- For production/large scale, use the Mongo Spark Connector or change stream approach.
"""

mongo_atlas_uri = "mongodb+srv://ls_db_user:s47tBnWikllAoe3k@democluster0.j777gry.mongodb.net/"
# S3_BUCKET = "s3://fq-app-analytics-bucket-1/iceberg-warehouse/ecommerce_db_raw/"

# -----------------------------------------------------------------------------
# Configuration (via ENV or defaults), 
# Iceberg / Glue / S3 config
# -----------------------------------------------------------------------------
MONGO_URI = os.environ.get("MONGO_URI", mongo_atlas_uri)
MONGO_DB = os.environ.get("MONGO_DB", "FQ_App")
COLLECTIONS = ["accounts", "close_tasks", "journal_entries"]
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
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

# Define schema for the accounts DataFrame
account_schema = StructType([
    StructField("account_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True)    	
    ])

# Define schema for the close_tasks DataFrame
close_tasks_schema = StructType([
    StructField("task_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("assigned_to", StringType(), True),
    StructField("status", StringType(), True),
    StructField("due_date", StringType(), True)
])

# Define schema for journal_entries_data (JSON array)
journal_schema = StructType([
    StructField("entry_id", StringType(), nullable=True),
    StructField("date", StringType(), nullable=True),
    StructField("lines", ArrayType(
        StructType([
            StructField("account_id", StringType(), nullable=True),
            StructField("debit", IntegerType(), nullable=True),
            StructField("credit", IntegerType(), nullable=True),
        ])
    ), nullable=True),
    StructField("description", StringType(), nullable=True)
])


SCHEMAS = {
    "account": (account_schema, "raw.fq_app_accounts"),
    "close_tasks": (close_tasks_schema, "raw.fq_app_close_tasks"),
    "journal_entries": (journal_schema, "raw.ecommerce_journal_entries"),
}


# -----------------------------------------------------------------------------
# Spark session builder with GlueCatalog (Iceberg) configuration
# -----------------------------------------------------------------------------
def create_spark_session(app_name="mongo-to-iceberg-ingestion"):
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
# check if table exists in the glue catalog
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
# 
# Stream documents from MongoDB (pymongo) in batches and write each batch into Iceberg table.
#     - If table doesn't exist: the first non-empty batch will create the table via writer.create()
#     - Subsequent batches use writer.append()
# -----------------------------------------------------------------------------
def data_ingestion(spark: SparkSession, collections=("accounts","close_tasks","journal_entries")):
    """
    Runs the Mongo->Iceberg extraction for listed collections.
    This function expects an active SparkSession (driver + executors set up).
    Raises exceptions on fatal errors.
    """
    logger.info("Starting Mongo ETL")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    namespace = "raw"

    try:
        for coll in collections:
            logger.info("Extracting collection %s", coll)
            # Example: iterate cursor in batches and write as in your original script
            cursor = db[coll].find({}, batch_size=BATCH_SIZE)
            docs_buffer = []
            created = False
            for doc in cursor:
                # normalize doc (use your normalize_bson)
                docs_buffer.append(normalize_bson(doc))
                if len(docs_buffer) >= BATCH_SIZE:
                    # convert to DataFrame using schema and write
                    df = spark.createDataFrame(docs_buffer)  # use schema variable
                    df = df.withColumn("ingest_ts", current_timestamp())
                    if not created:
                        # create table via writeTo().create() or create DDL
                        df.writeTo(f"{GLUE_CATALOG_NAME}.{namespace}.{coll}").create()
                        created = True
                    else:
                        df.writeTo(f"{GLUE_CATALOG_NAME}.{namespace}.{coll}").append()
                    docs_buffer = []
            # leftover
            if docs_buffer:
                df = spark.createDataFrame(docs_buffer)
                df = df.withColumn("ingest_ts", current_timestamp())
                if not created:
                    df.writeTo(f"{GLUE_CATALOG_NAME}.{namespace}.{coll}").create()
                else:
                    df.writeTo(f"{GLUE_CATALOG_NAME}.{namespace}.{coll}").append()
            logger.info("Finished ingesting %s", coll)
    except Exception:
        logger.exception("Failed while ingesting collections")
        raise
    finally:
        client.close()

    logger.info("Mongo ETL completed successfully")

def main():
    # entrypoint if run standalone
    spark = create_spark_session()
    try:
        data_ingestion(spark)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
