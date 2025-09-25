#!/usr/bin/env python3
# src\curated_silver_fqapp.py
import os
import logging
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_date, current_timestamp, year, month, expr, coalesce
from pyspark.sql.window import Window

# ---------- Configuration ----------
GLUE_CATALOG = os.environ.get("GLUE_CATALOG_NAME", "iceberg")
RAW_NS = os.environ.get("RAW_NAMESPACE", "raw_data")
CURATED_NS = os.environ.get("CURATED_NAMESPACE", "curated")
APP = os.environ.get("APP_NAME", "fq_app")
S3_BUCKET = os.environ.get("S3_BUCKET", "")
WAREHOUSE = os.environ.get("WAREHOUSE", f"s3a://{S3_BUCKET}/iceberg-warehouse/") if S3_BUCKET else None

# Partitioning specs (strings for DDL)
PARTITION_BY_DATE = "PARTITIONED BY (years(entry_date), months(entry_date))"  # year/month partitioning
PARTITION_BY_DUE = "PARTITIONED BY (years(due_date), months(due_date))"


# Table properties for compaction & performance (tuning the infra)
TABLE_PROPERTIES = {
    "write.target-file-size": "536870912",            # ~512 MB target parquet files
    "format-version": "2",                            # Iceberg format v2 (recommended)
    "write.metadata.previous-versions-max": "5",      # keep fewer previous manifest lists
    "read.split.target-size": "134217728"             # ~128 MB read split target
}

# retention defaults for snapshots
DEFAULT_EXPIRE_DAYS = int(os.environ.get("EXPIRE_SNAPSHOTS_DAYS", "30"))
DEFAULT_RETAIN_LAST = int(os.environ.get("RETAIN_LAST_SNAPSHOTS", "3"))

logger = logging.getLogger("curated_jobs")
logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))


# -----------------------------------------------------------------------------
# Spark session builder with Iceberg and AWS Glue Catalog
# -----------------------------------------------------------------------------
def spark_session(app_name="curated-ddl"):
    b = SparkSession.builder.appName(app_name) \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{GLUE_CATALOG}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{GLUE_CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    if WAREHOUSE:
        b = b.config(f"spark.sql.catalog.{GLUE_CATALOG}.warehouse", WAREHOUSE)
    if os.environ.get("AWS_REGION"):
        b = b.config("spark.hadoop.fs.s3a.region", os.environ["AWS_REGION"])
    if os.environ.get("AWS_ACCESS_KEY_ID"):
        b = b.config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"]) \
             .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    return b.getOrCreate()

# -----------------------------------------------------------------------------
# Helper to add table properties to the DDL.
# Configure table properties for compaction. 
# -----------------------------------------------------------------------------
def build_properties_clause(props: dict) -> str:
    if not props:
        return ""
    pairs = ",\n  ".join(f"'{k}' = '{v}'" for k, v in props.items())
    return f"TBLPROPERTIES (\n  {pairs}\n)"

def run_ddl(spark: SparkSession, ddl: str):
    logger.info("Running DDL:\n%s", ddl)
    spark.sql(ddl)

def create_namespace_if_missing(spark: SparkSession):
    try:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {GLUE_CATALOG}.{CURATED_NS}")
    except Exception:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {GLUE_CATALOG}.{CURATED_NS}")

# -----------------------------------------------------------------------------
# Snapshot expiry & vacuum: 
# Implement data retention policies using Iceberg snapshots
# -----------------------------------------------------------------------------
def expire_snapshots_and_vacuum(spark: SparkSession, qualified_table: str,
                                older_than_days: int = DEFAULT_EXPIRE_DAYS,
                                retain_last: int = DEFAULT_RETAIN_LAST):
    """
    Expire old Iceberg snapshots and remove orphan files.
    Uses Iceberg SQL procedures if available. 
      1) expires snapshots older than cutoff (keeps 'retain_last' snapshots),
      2) removes orphan files left behind.
    Impact: reclaims S3 storage and reduces metadata history; irreversible for the expired snapshots.
    """
    cutoff = (datetime.utcnow() - timedelta(days=older_than_days)).isoformat()
    # Examples of Iceberg procedure signatures differ across versions; try multiple forms
    called = False
    # 1 - Try v2-style catalog procedure with named params
    try:
        sql = (f"CALL {GLUE_CATALOG}.system.expire_snapshots("
               f"table => '{qualified_table}', older_than => TIMESTAMP '{cutoff}', retain_last => {retain_last})")
        logger.info("Calling expire_snapshots: %s", sql)
        spark.sql(sql)
        called = True
    except Exception as e:
        logger.debug("expire_snapshots catalog call failed (v2 style): %s", e)

    # 2 - Try generic system.expire_snapshots(table => '...', older_than => TIMESTAMP '...', retain_last => N)
    if not called:
        try:
            sql = (f"CALL system.expire_snapshots(table => '{qualified_table}', older_than => TIMESTAMP '{cutoff}', retain_last => {retain_last})")
            logger.info("Calling expire_snapshots fallback: %s", sql)
            spark.sql(sql)
            called = True
        except Exception as e:
            logger.debug("expire_snapshots fallback failed: %s", e)

    # After expiring snapshots, try removing orphan files
    try:
        # common form: CALL <catalog>.system.remove_orphan_files(table => '<tbl>', older_than => TIMESTAMP '...')
        sql_rm = (f"CALL {GLUE_CATALOG}.system.remove_orphan_files(table => '{qualified_table}')")
        logger.info("Calling remove_orphan_files: %s", sql_rm)
        spark.sql(sql_rm)
    except Exception as e:
        logger.debug("remove_orphan_files call failed or not supported: %s", e)

    logger.info("Expire/vacuum attempted for %s (older_than_days=%d, retain_last=%d)", qualified_table, older_than_days, retain_last)

# -----------------------------------------------------------------------------
# Create curated tables with DDL including properties
# -----------------------------------------------------------------------------
    # created_at TIMESTAMP,
    # create_user STRING,
    # updated_at TIMESTAMP,
    # update_user STRING

close_tasks_columns = """
    _id STRING,
    task_id STRING,
    name STRING,
    assigned_to STRING,
    status STRING,
    due_date DATE,
    ingest_ts TIMESTAMP
    """

journal_entries_silver_columns = """
    _id STRING,
    entry_id STRING,
    entry_date DATE,
    description STRING,
    account_id STRING,
    account_name STRING,
    account_type STRING,
    debit DOUBLE,
    credit DOUBLE,
    ingest_ts TIMESTAMP
    """

journal_entries_gold_columns = """
    entry_id STRING,
    entry_date DATE,
    total_credit DOUBLE,
    total_debit DOUBLE
    """

# -----------------------------------------------------------------------------
# Create curated tables with DDL including properties
# -----------------------------------------------------------------------------
def create_curated_tables_fq_app(spark: SparkSession):

    close_tbl = f"{GLUE_CATALOG}.{CURATED_NS}.{APP}_close_tasks_silver"
    journal_silver_tbl = f"{GLUE_CATALOG}.{CURATED_NS}.{APP}_journal_entries_silver"
    journal_gold_tbl = f"{GLUE_CATALOG}.{CURATED_NS}.{APP}_journal_entries_gold"

    ddl_close = f"""
    CREATE TABLE IF NOT EXISTS {close_tbl} (
      {close_tasks_columns}
    )
    USING iceberg
    PARTITIONED BY (months(due_date))
    {build_properties_clause(TABLE_PROPERTIES)}
    """
    run_ddl(spark, ddl_close)

    ddl_journal_silver = f"""
    CREATE TABLE IF NOT EXISTS {journal_silver_tbl} (
      {journal_entries_silver_columns}
    )
    USING iceberg
    PARTITIONED BY (months(entry_date))
    {build_properties_clause(TABLE_PROPERTIES)}
    """
    run_ddl(spark, ddl_journal_silver)

    ddl_journal_gold = f"""
    CREATE TABLE IF NOT EXISTS {journal_gold_tbl} (
      {journal_entries_gold_columns}
    )
    USING iceberg
    PARTITIONED BY (years(entry_date))
    {build_properties_clause(TABLE_PROPERTIES)}
    """
    run_ddl(spark, ddl_journal_gold)

# -----------------------------------------------------------------------------
# Transform raw data, deduplication logic based on primary keys and write to curated tables
# close_tasks_silver - date string is transformed to date. 
# jounal_entries_silver - Flatten nested journal_entries.lines into separate rows & join with account table for denormalised view. 
# jounal_entries_gold - with total credit and total debit per journal entry. 
# -----------------------------------------------------------------------------
def build_and_write_curated(spark: SparkSession):
    """
    Read raw tables, transform and write to curated silver and gold tables.
    Implements deduplication and upsert logic using MERGE statements.
    - Transform raw close_tasks into curated silver:
    - convert date strings to date type,
    - dedupe on primary key (task_id),
    - add partition columns: year, month based on due_date,
    - write as Iceberg table (create if missing then MERGE for incremental)
    """
    raw_journal = f"{GLUE_CATALOG}.{RAW_NS}.{APP}_journal_entries"
    raw_accounts = f"{GLUE_CATALOG}.{RAW_NS}.{APP}_accounts"
    raw_close = f"{GLUE_CATALOG}.{RAW_NS}.{APP}_close_tasks"

    # --- close tasks silver ---
    df_close_tasks = spark.table(raw_close) \
        .withColumn("due_date", to_date(col("due_date"))) \
        .withColumn("ingest_ts", coalesce(col("ingest_ts"), current_timestamp())) 
    w = Window.partitionBy("task_id").orderBy(col("ingest_ts").desc())
    df_close_dedup = df_close_tasks.withColumn("_rn", expr("row_number() over (partition by task_id order by ingest_ts desc)")).filter(col("_rn")==1).drop("_rn")

    target_close = f"{GLUE_CATALOG}.{CURATED_NS}.{APP}_close_tasks_silver"
    df_close_dedup.createOrReplaceTempView("_stg_close_tasks")
    merge_sql = f"""
      MERGE INTO {target_close} t
      USING _stg_close_tasks s
      ON t.task_id = s.task_id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_sql)
    # expire snapshots for close tasks
    expire_snapshots_and_vacuum(spark, target_close)

    # --- journal entries silver ---
    df_journal = spark.table(raw_journal)
    df_account = spark.table(raw_accounts)
    df_journal_flattened = df_journal.select(
        col("_id"), col("entry_id"), to_date(col("date")).alias("entry_date"),
        col("description"), explode(col("lines")).alias("line"), col("ingest_ts")
    )
    df_journal_lines = df_journal_flattened.select(
        col("_id"), col("entry_id"), col("entry_date"),
        col("description"), col("line.account_id").alias("account_id"),
        col("line.debit").alias("debit"), col("line.credit").alias("credit"),
        col("ingest_ts") )

    df_joined = df_journal_lines.join(df_account.select("account_id", "name", "type"), on="account_id", how="left") \
        .withColumnRenamed("name", "account_name") \
        .withColumnRenamed("type", "account_type")

    df_j_dedup = df_joined.withColumn("_rn", expr("row_number() over (partition by entry_id, account_id order by ingest_ts desc)")).filter(col("_rn")==1).drop("_rn")

    target_journal_silver = f"{GLUE_CATALOG}.{CURATED_NS}.{APP}_journal_entries_silver"
    df_j_dedup.createOrReplaceTempView("_stg_journal_silver")
    merge_j_s = f"""
      MERGE INTO {target_journal_silver} t
      USING _stg_journal_silver s
      ON t.entry_id = s.entry_id AND t.account_id = s.account_id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_j_s)
    expire_snapshots_and_vacuum(spark, target_journal_silver)

    # --- journal entries gold ---
    df_silver = spark.table(target_journal_silver)
    agg = df_silver.groupBy("entry_id", "entry_date").agg(
        expr("sum(coalesce(credit,0)) as total_credit"),
        expr("sum(coalesce(debit,0)) as total_debit")
        # expr("max(ingest_ts) as ingest_ts")
        )

    target_gold = f"{GLUE_CATALOG}.{CURATED_NS}.{APP}_journal_entries_gold"
    agg.createOrReplaceTempView("_stg_journal_gold")
    merge_gold_sql = f"""
      MERGE INTO {target_gold} t
      USING _stg_journal_gold s
      ON t.entry_id = s.entry_id
      WHEN MATCHED THEN UPDATE SET *
      WHEN NOT MATCHED THEN INSERT *
    """
    spark.sql(merge_gold_sql)
    expire_snapshots_and_vacuum(spark, target_gold, older_than_days=90, retain_last=5)

# -----------------------------------------------------------------------------
# run curated job entrypoint
# -----------------------------------------------------------------------------
def run_curated(spark: SparkSession):
    """High-level function the runner or CLI calls."""
    logger.info("Curated Silver JObs.")
    create_namespace_if_missing(spark)
    create_curated_tables_fq_app(spark)
    build_and_write_curated(spark)
    logger.info("Curated jobs finished.")
# -----------------------------------------------------------------------------
# Main entrypoint if run standalone
# -----------------------------------------------------------------------------
def main():
    spark = spark_session()
    try:
        create_namespace_if_missing(spark)
        create_curated_tables_fq_app(spark)
        build_and_write_curated(spark)
        logger.info("Curated jobs finished.")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
