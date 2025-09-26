#!/usr/bin/env python3
"""
src\mongodb_iceberg_fqapp.py
POC ETL: extract collections from MongoDB (using pymongo) and write into Iceberg tables
backed by AWS Glue Catalog. Uses Spark 3.5.x DataFrameWriterV2 (writeTo API).

- This script streams documents in batches from MongoDB to avoid loading everything into memory.
- For production/large scale, use the Mongo Spark Connector or change stream approach.

Usage:
  - Provide env vars (via docker-compose or exported):
      MONGO_URI, MONGO_DB, APP_NAME, S3_BUCKET, GLUE_CATALOG_NAME, AWS_REGION, BATCH_SIZE
  - Called by runner that sets up SparkSession, or run standalone:
      spark-submit --packages "<packages...>" src/mongo_etl.py
      
"""

import os
import sys
import time
import logging
import json
import decimal
from typing import List, Dict, Any, Optional, Iterator
from bson import ObjectId
from datetime import datetime, date
from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, TimestampType, ArrayType, IntegerType, DecimalType, BooleanType)
from pyspark.sql.utils import AnalysisException

# -------------------------
# Logging
# -------------------------
logger = logging.getLogger("mongodb_iceberg_fqapp.py.py")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))

mongo_atlas_uri = "mongodb+srv://ls_db_user:s47tBnWikllAoe3k@democluster0.j777gry.mongodb.net/"
# S3_BUCKET = "s3://fq-app-analytics-bucket-1/iceberg-warehouse/ecommerce_db_raw/"

# -----------------------------------------------------------------------------
# Configuration (via ENV or defaults), 
# Iceberg / Glue / S3 config
# -----------------------------------------------------------------------------
MONGO_URI = os.environ.get("MONGO_URI", mongo_atlas_uri)
MONGO_DB = os.environ.get("MONGO_DB", "FQ_App")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "1000"))
GLUE_CATALOG_NAME = os.environ.get("GLUE_CATALOG_NAME", "iceberg")  # logical name used in spark configs
AWS_REGION = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-2"))
S3_BUCKET = os.environ.get("S3_BUCKET", "fq-app-analytics-bucket-1")  # replace with actual bucket if not set
WAREHOUSE = os.environ.get("WAREHOUSE", f"s3a://{S3_BUCKET}/iceberg-warehouse/")
APP_NAME = os.environ.get("APP_NAME", "fq_app")
NAMESPACE = os.environ.get("NAMESPACE", "raw_data")
CLEAN_RUN = os.environ.get("CLEAN_RUN", "false").lower() == "true"

# -----------------------------------------------------------------------------
# Define schemas for the mongodb collections
# -----------------------------------------------------------------------------
money_type = DecimalType(18, 2)

# Define schema for the accounts DataFrame
account_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("account_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("type", StringType(), True)
    ])

# Define schema for the close_tasks DataFrame
close_tasks_schema = StructType([
    StructField("_id", StringType(), True),
    StructField("task_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("assigned_to", StringType(), True),
    StructField("status", StringType(), True),
    StructField("due_date", StringType(), True)
])

# Define schema for journal_entries_data (JSON array)
journal_schema = StructType([
    StructField("_id", StringType(), True),
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

# accounts_ddl = f"""
#     CREATE TABLE IF NOT EXISTS {CURATED_QUALIFIED} (
#         _id STRING,
#         account_id STRING,
#         name STRING,
#         type STRING,
#         ingest_ts TIMESTAMP
#     )
#     USING iceberg
#     PARTITIONED BY (days(order_date))
#     """

# close_tasks_schema = f"""
#     CREATE TABLE IF NOT EXISTS {CURATED_QUALIFIED} (
#         _id STRING,
#         task_id STRING,
#         name STRING,
#         assigned_to STRING,
#         status STRING,
#         due_date STRING,
#         ingest_ts TIMESTAMP
#     )
#     USING iceberg
#     PARTITIONED BY (days(order_date))
#     """

# schema_ddl = """
#        _id STRING,
#        entry_id STRING,
#        date STRING,
#        lines ARRAY<STRUCT<account_id:STRING, debit:DECIMAL(18,2), credit:DECIMAL(18,2)>>,
#        ingest_ts TIMESTAMP
#     """
#     partition_sql = "PARTITIONED BY (date)"
#     s3_location = f"s3a://{os.environ.get('S3_BUCKET')}/iceberg-warehouse/raw/{app_name}/{coll}/"


# small dimension table — no partitioning is fine, or partition by category if high-cardinality is fine
# define the schema
COLLECTION_SCHEMAS = {
    "accounts": {
        "schema": account_schema,
        "partition": "days(ingest_ts)",
        "location": "s3a://fq-app-analytics-bucket-1/iceberg-warehouse/raw/fq_app/accounts/"
    },
    "close_tasks": {
        "schema": close_tasks_schema,
        "partition": "days(ingest_ts)",
        "location": "s3a://fq-app-analytics-bucket-1/iceberg-warehouse/raw/fq_app/close_tasks/"
    },
    "journal_entries": {
        "schema": journal_schema,
        "partition": "days(ingest_ts)",
        "location": "s3a://fq-app-analytics-bucket-1/iceberg-warehouse/raw/fq_app/journal_entries/"
    }
}

# -----------------------------------------------------------------------------
#Helpers - convert StructType → DDL string
# -----------------------------------------------------------------------------
def spark_type_to_ddl(t):
    """
    Convert Spark data type to Iceberg-compatible SQL type expression (simple mapping).
    """
    from pyspark.sql.types import StringType, IntegerType, LongType, DoubleType, FloatType, BooleanType, DecimalType, TimestampType, DateType, ArrayType, StructType

    if isinstance(t, StringType):
        return "STRING"
    if isinstance(t, IntegerType) or isinstance(t, LongType):
        return "INT"
    if isinstance(t, DoubleType) or isinstance(t, FloatType):
        return "DOUBLE"
    if isinstance(t, DecimalType):
        return f"DECIMAL({t.precision},{t.scale})"
    if isinstance(t, BooleanType):
        return "BOOLEAN"
    if isinstance(t, TimestampType):
        return "TIMESTAMP"
    if isinstance(t, DateType):
        return "DATE"
    if isinstance(t, ArrayType):
        elem = spark_type_to_ddl(t.elementType)
        return f"ARRAY<{elem}>"
    if isinstance(t, StructType):
        inner = ", ".join(f"{f.name}:{spark_type_to_ddl(f.dataType)}" for f in t.fields)
        return f"STRUCT<{inner}>"
    # fallback
    return "STRING"

# Convert StructType -> DDL column list string for CREATE TABLE.
def struct_to_ddl(schema: StructType):
    parts = []
    for f in schema.fields:
        tddl = spark_type_to_ddl(f.dataType)
        parts.append(f"{f.name} {tddl}")
    return ",\n".join(parts)

# -----------------------------------------------------------------------------
# Convert common MongoDB BSON types to Python/Spark friendly DataFrame creation
# -----------------------------------------------------------------------------
def normalize_bson_value(v):
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, datetime):
        return v  # pyspark accepts python datetime for TimestampType
    if isinstance(v, date) and not isinstance(v, datetime):
        return v
    if isinstance(v, dict):
        return {k: normalize_bson_value(vv) for k, vv in v.items()}
    if isinstance(v, list):
        return [normalize_bson_value(x) for x in v]
    # leave primitives as-is
    return v

def normalize_doc_for_spark(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Walk a Mongo document and convert BSON types to Spark-friendly types (ObjectId->str, etc.)."""
    return {k: normalize_bson_value(v) for k, v in doc.items()}

# -----------------------------------------------------------------------------
# Check DB and table existence, create if needed
# -----------------------------------------------------------------------------
def ensure_database_and_table(
    spark: SparkSession,
    catalog: str,
    namespace: str,
    table_name: str,
    schema_ddl: Optional[str] = None,
    location: Optional[str] = None,
    partition_sql: Optional[str] = None
):
    """
    Ensure namespace exists and create table if schema_ddl provided.
    Uses CREATE TABLE IF NOT EXISTS so it's safe to call repeatedly (idempotent).
    """
    qualified_db = f"{catalog}.{namespace}"
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {qualified_db}")
    except Exception:
        # Some Spark/Iceberg combos use CREATE NAMESPACE; try that
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {qualified_db}")

    qualified_table = f"{catalog}.{namespace}.{table_name}"

    if schema_ddl:
        ddl = f"CREATE TABLE IF NOT EXISTS {qualified_table} (\n    {schema_ddl}\n) USING iceberg"
        if partition_sql:
            ddl += f"\n{partition_sql}"
        if location:
            ddl += f"\nLOCATION '{location}'"
        logger.info("Running DDL to ensure table exists: %s", ddl.replace("\n", " "))
        spark.sql(ddl)

# -----------------------------------------------------------------------------
# Crrate table if missing, else append
# -----------------------------------------------------------------------------
def safe_create_or_append_df(
    spark: SparkSession,
    df,
    catalog: str,
    namespace: str,
    table_name: str,
    schema_ddl: Optional[str] = None,
    location: Optional[str] = None,
    partition_sql: Optional[str] = None,
    clean_run=False
):
    """
    Idempotent create-or-append logic:
      - If schema_ddl provided: run CREATE TABLE IF NOT EXISTS ... (DDL approach) then append
      - Else: attempt df.writeTo(...).create(); if race/exists -> append
    """

    qualified_table = f"{catalog}.{namespace}.{table_name}"

    if clean_run:
        # Drop from catalog (and S3, with PURGE)
        spark.sql(f"DROP TABLE IF EXISTS {qualified_table} PURGE")
        # spark.sql("DROP TABLE IF EXISTS iceberg.raw.fq_app_close_tasks PURGE")
        logger.info("Clean run: dropped table %s if it existed", qualified_table)

    # Ensure namespace/table if DDL is available
    if schema_ddl:
        ensure_database_and_table(spark, catalog, namespace, table_name, schema_ddl, location, partition_sql)
    try:
        # If table exists -> append
        try:
            spark.table(qualified_table)
            table_exists = True
        except AnalysisException:
            table_exists = False

        if table_exists:
            logger.info("Table %s exists. Appending data (%d rows approx)", qualified_table, df.count() if hasattr(df, "count") else -1)
            df.writeTo(qualified_table).append()
        else:
            if schema_ddl:
                # If we created via DDL above then just append
                logger.info("Table %s created via DDL; appending.", qualified_table)
                df.writeTo(qualified_table).append()
            else:
                # Try to create using DataFrame schema
                logger.info("Table %s does not exist. Creating from DataFrame schema.", qualified_table)
                try:
                    df.writeTo(qualified_table).create()
                except AnalysisException as ae:
                    # Race condition: another process created the table concurrently
                    logger.warning("Create failed (possible race). Falling back to append. %s", ae)
                    df.writeTo(qualified_table).append()
    except Exception:
        logger.exception("Failed to write to table %s", qualified_table)
        raise

# -------------------------
# Mongo fetch logic (using _id paging)
# -------------------------
def fetch_from_mongo_in_batches_paged(mongo_uri: str, db_name: str, collection_name: str,
                                      batch_size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
    """
    Page through collection using _id range queries. Yields lists of normalized docs.
    Maintains low server-side cursor lifetime.
    recommendadtion to persist last_id to S3/DynamoDB if you want resumable incremental ingestion.
    """
    client = MongoClient(mongo_uri)
    try:
        coll = client[db_name][collection_name]
        last_id = None
        while True:
            query = {"_id": {"$gt": last_id}} if last_id is not None else {}
            # sort by _id ascending to page forward
            cursor = coll.find(query).sort("_id", 1).limit(batch_size)
            batch = []
            for doc in cursor:
                batch.append(normalize_doc_for_spark(doc))
            if not batch:
                break
            # update last_id to the last _id from this batch
            last_id = batch[-1].get("_id")
            # if original _id was an ObjectId, ensure we pass ObjectId back
            if isinstance(last_id, str):
                try:
                    last_id = ObjectId(last_id)
                except Exception:
                    # keep as-is if not ObjectId
                    pass
            yield batch
    finally:
        client.close()

# -------------------------
# Mongo fetch logic (batched)
# -------------------------
def fetch_from_mongo_in_batches(mongo_uri: str, db_name: str, collection_name: str, batch_size: int = 1000):
    """
    Generator yielding lists of normalized docs per batch.
    """
    client = MongoClient(mongo_uri)
    try:
        coll = client[db_name][collection_name]
        cursor = coll.find({}, batch_size=batch_size)
        batch = []
        for doc in cursor:
            batch.append(normalize_doc_for_spark(doc))
            if len(batch) >= batch_size:
                yield batch
                batch = []
        if batch:
            yield batch
        cursor.close()
    finally:
        client.close()

# -----------------------------------------------------------------------------
# check if table exists in the glue catalog
# -----------------------------------------------------------------------------
# def ensure_database_and_table(spark, catalog, namespace, table_name, schema_ddl=None, location=None, partition_sql=None):
#     """
#     Ensure namespace (database) exists and create table if it doesn't.
#     - catalog: e.g. "iceberg"
#     - namespace: e.g. "raw"
#     - table_name: e.g. "fq_app_orders"
#     - schema_ddl (optional): string of column definitions for CREATE TABLE
#     - location (optional): explicit s3a path to use for LOCATION
#     - partition_sql (optional): e.g. "PARTITIONED BY (days(order_date))"
#     """
#     qualified_db = f"{catalog}.{namespace}"
#     try:
#         spark.sql(f"CREATE DATABASE IF NOT EXISTS {qualified_db}")
#     except Exception as e:
#         # Some catalogs use CREATE NAMESPACE
#         logger.warning("CREATE DATABASE failed, trying CREATE NAMESPACE: %s", e)
#         spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {qualified_db}")

#     qualified_table = f"{catalog}.{namespace}.{table_name}"

#     # If schema_ddl provided, create with explicit DDL )
#     if schema_ddl:
#         ddl = f"CREATE TABLE IF NOT EXISTS {qualified_table} ({schema_ddl}) USING iceberg"
#         if partition_sql:
#             ddl += f" {partition_sql}"
#         if location:
#             ddl += f" LOCATION '{location}'"
#         logger.info("Running DDL: %s", ddl)
#         spark.sql(ddl)
#         return

#     # otherwise, rely on writer.create() in caller
#     return

# -----------------------------------------------------------------------------
# Assumes SparkSession is already initialized as per the previous discussion
# with the correct Glue Catalog configuration
# -----------------------------------------------------------------------------
def perform_clean_run(spark, table_name ):
    """
    Performs a clean-up of the target Iceberg table by dropping and purging
    its metadata and data, then removing any potential orphaned files.
    """
    TABLE_NAME = table_name  # e.g. "iceberg.raw.fq_app_orders"
    print(f"Dropping and purging table: {TABLE_NAME}")
    try:
        # Step 1: Drop the table and purge its data
        spark.sql(f"DROP TABLE IF EXISTS {TABLE_NAME} PURGE")
    except Exception as e:
        print(f"Failed to drop table {TABLE_NAME}: {e}")

    # Step 2: Clean up any potential orphaned files
    print("Running orphan file cleanup...")
    try:
        # Define a timestamp to protect recent files
        from datetime import datetime, timedelta
        older_than = datetime.now() - timedelta(days=3)
        older_than_str = older_than.strftime("%Y-%m-%d %H:%M:%S")

        # Execute the stored procedure, CALL catalog_name.system.remove_orphan_files(table => 'db.sample', dry_run => true);
        spark.sql(f"""
            CALL iceberg.system.remove_orphan_files(
                table =>'{TABLE_NAME.split('.', 1)[1]}',  
                older_than => TIMESTAMP '{older_than_str}'
            )
        """)
    except Exception as e:
        print(f"Orphan file cleanup failed: {e}")

# Example usage in your ETL script
# perform_clean_run(spark)
# Run your ingestion logic here...

# -----------------------------------------------------------------------------
# Core: ingest collection in batches using pymongo cursor
# 
# Stream documents from MongoDB (pymongo) in batches and write each batch into Iceberg table.
#     - If table doesn't exist: the first non-empty batch will create the table via writer.create()
#     - Subsequent batches use writer.append()
# collections=("accounts","close_tasks","journal_entries")
# -----------------------------------------------------------------------------
def data_ingestion(spark: SparkSession, collections: Optional[List[str]] = None):
    """
    Main ingestion loop: for each collection in COLLECTION_SCHEMAS (or provided list),
    read Mongo in batches and write to Iceberg with safe_create_or_append_df.
    """
    collections = collections or list(COLLECTION_SCHEMAS.keys())
    logger.info("Starting ingestion for collections: %s", collections)
    catalog = GLUE_CATALOG_NAME
    namespace = NAMESPACE
    app = APP_NAME
    # clean_run = CLEAN_RUN

    # if clean_run:
    #     logger.info("Clean run enabled: dropping existing tables in namespace %s", namespace) 
    #     spark.sql("DROP NAMESPACE iceberg.raw CASCADE")
    #     # # spark.sql("DROP TABLE IF EXISTS iceberg.raw.fq_app_accounts PURGE")
    #     # # spark.sql("DROP TABLE IF EXISTS iceberg.raw.fq_app_close_tasks PURGE")
    #     # # spark.sql("DROP TABLE IF EXISTS iceberg.raw.fq_app_journal_entries PURGE")
    #     # perform_clean_run(spark, f"{catalog}.{namespace}.{app}_accounts")
    #     # perform_clean_run(spark, f"{catalog}.{namespace}.{app}_close_tasks")
    #     # perform_clean_run(spark, f"{catalog}.{namespace}.{app}_journal_entries")
    #     logger.info("Clean run: dropped tables in namespace %s if they existed", namespace)

    for coll in collections:
        if coll not in COLLECTION_SCHEMAS:
            logger.warning("No schema config for collection '%s', skipping.", coll)
            continue

        coll_items = COLLECTION_SCHEMAS[coll]
        schema_struct = coll_items.get("schema")           # StructType (preferred)
        schema_ddl = coll_items.get("schema_ddl")         # explicit DDL (optional)
        partition = coll_items.get("partition")           # e.g. "days(ingest_ts)"
        location = coll_items.get("location")
        table_name = f"{app}_{coll}"

        logger.info("Ingesting collection=%s -> table=%s.%s.%s", coll, catalog, namespace, table_name)

        # For control & safety, ensure ingest_ts present on every record we create
        batches_processed = 0
        for batch_docs in fetch_from_mongo_in_batches(MONGO_URI, MONGO_DB, coll, batch_size=BATCH_SIZE):
            if not batch_docs:
                continue

            # Create DataFrame using provided StructType (if available) or infer
            if schema_struct is not None:
                df = spark.createDataFrame(batch_docs, schema=schema_struct)
                # df_account = spark.read.option("multiline", "true").schema(account_schema).json('/app/data/accounts.json')    
            else:
                df = spark.createDataFrame(batch_docs)

            # Ensure ingest_ts column exists for partitioning/deduping
            if "ingest_ts" not in df.columns:
                df = df.withColumn("ingest_ts", current_timestamp())

            # If no explicit DDL but StructType provided, derive DDL for create-if-missing (optional)
            final_schema_ddl = schema_ddl
            if final_schema_ddl is None and schema_struct is not None:
                try:
                    # get ddl from struct
                    final_schema_ddl = struct_to_ddl(schema_struct)
                    # add ingest_ts:
                    final_schema_ddl = final_schema_ddl + ",\n    ingest_ts TIMESTAMP"                                      
                except Exception as e:
                    logger.warning("Failed converting StructType to DDL for %s: %s", coll, e)
                    final_schema_ddl = None

            # Build partition if provided, PARTITIONED BY (days(ingest_ts)) etc.
            partition_sql = f"PARTITIONED BY ({partition})" if partition else None

            # Attempt safe create or append
            safe_create_or_append_df(
                spark=spark,
                df=df,
                catalog=catalog,
                namespace=namespace,
                table_name=table_name,
                schema_ddl=final_schema_ddl,
                location=location,
                partition_sql=partition_sql,
                clean_run=CLEAN_RUN
            )
            batches_processed += 1
            logger.info("Wrote batch #%d for collection %s (approx %d rows)", batches_processed, coll, len(batch_docs))

        logger.info("Completed ingestion for collection=%s; batches=%d", coll, batches_processed)

# -----------------------------------------------------------------------------
# Spark session builder with GlueCatalog (Iceberg) configuration, if running standalone
# icebergs configs should be passed as spark-submit --conf too
# -----------------------------------------------------------------------------
def create_spark_session(app_name="mongo-to-iceberg-ingestion"):
    builder = SparkSession.builder.appName(app_name)
    """
    Create SparkSession configured for Iceberg + GlueCatalog + S3
    Note: it's common to provide --packages on spark-submit to download jars (or bake jars into image).
    """
    builder = SparkSession.builder.appName(app_name)
    builder = builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{GLUE_CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{GLUE_CATALOG_NAME}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config(f"spark.sql.catalog.{GLUE_CATALOG_NAME}.warehouse", WAREHOUSE) \
        .config("spark.hadoop.fs.s3a.region", AWS_REGION) \
        .config("spark.driver.extraJavaOptions", f"-Daws.region={AWS_REGION}") \
        .config("spark.executor.extraJavaOptions", f"-Daws.region={AWS_REGION}")
    # You may want to configure s3a access key/secret here or rely on env/IAM
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        builder = builder.config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"]) \
                         .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    return builder.getOrCreate()

    # spark = SparkSession.builder \
    # .appName("etl") \
    # .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    # .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    # .config("spark.sql.catalog.iceberg.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    # .config("spark.sql.catalog.iceberg.warehouse", "s3a://fq-app-analytics-bucket-1/iceberg-warehouse/") \
    # .getOrCreate()

# -----------------------------------------------------------------------------
# main if run standalone
# -----------------------------------------------------------------------------
def main():
    # entrypoint if run standalone
    spark = None
    try:
        spark = create_spark_session("mongo-etl-runner-standalone")
        data_ingestion(spark)
        logger.info("All collections ingested successfully.")
    except Exception:
        logger.exception("ETL failed.")
        raise
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                logger.exception("Error stopping SparkSession")

if __name__ == "__main__":
    main()

    

