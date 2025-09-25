# src/runner_etl.py
import os
import sys
import time
import logging
from pyspark.sql import SparkSession
from importlib import import_module

logger = logging.getLogger("runner_ingestion")
logging.basicConfig(level=os.environ.get("LOG_LEVEL","INFO"))


# -----------------------------------------------------------------------------
# Helper - Retry utility
# -----------------------------------------------------------------------------
def retry(func, retries=3, base_delay=5, backoff=2, *args, **kwargs):
    last_exc = None
    delay = base_delay
    for i in range(1, retries+1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exc = e
            logger.warning("Attempt %d/%d failed: %s", i, retries, str(e))
            if i < retries:
                logger.info("Sleeping %ds before retry", delay)
                time.sleep(delay)
                delay *= backoff
            else:
                logger.error("All %d attempts failed.", retries)
    raise last_exc

# -----------------------------------------------------------------------------
# Spark session builder with Iceberg and AWS Glue Catalog
# -----------------------------------------------------------------------------
def create_spark():
    # Important: packages should be provided by spark-submit --packages OR baked into image.
    catalog = os.environ.get("GLUE_CATALOG_NAME", "iceberg")
    warehouse = os.environ.get("WAREHOUSE", f"s3a://{os.environ.get('S3_BUCKET','fq-app-analytics-bucket-1')}/iceberg-warehouse/")
    aws_region = os.environ.get("AWS_REGION", os.environ.get("AWS_DEFAULT_REGION", "us-east-2"))

    builder = SparkSession.builder.appName("mongo-to-iceberg-ingestion")
    # Do not set spark.jars.packages here if spark-submit passes them; but safe to include if needed.
    builder = builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog") \
        .config(f"spark.sql.catalog.{catalog}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse) \
        .config("spark.hadoop.fs.s3a.region", aws_region) \
        .config("spark.driver.extraJavaOptions", f"-Daws.region={aws_region}") \
        .config("spark.executor.extraJavaOptions", f"-Daws.region={aws_region}")
    spark = builder.getOrCreate()
    return spark

# -----------------------------------------------------------------------------
# Main ETL runner for MongoDB to Iceberg Ingestion & curated silver/gold tables
# -----------------------------------------------------------------------------
def main():
    exit_code = 0
    spark = None
    try:
        spark = create_spark()

        # import jobs
        mongo_job = import_module("mongodb_iceberg_fqapp")
        curated_job = import_module("curated_silver_fqapp")

        # 1 - Run mongo extraction with retries (transient network issues)
        run_curated_flag = os.environ.get("RUN_CURATED", "true").lower() in ("1","true","yes")

        logger.info("Starting mongo ETL step")
        retry(lambda: mongo_job.data_ingestion(spark), retries=int(os.environ.get("ETL_RETRIES", "3")), base_delay=5)

        # 2 - Run curated job (optionally)
        if run_curated_flag:
            logger.info("Starting curated (silver) step")
            retry(lambda: curated_job.run_curated(spark), retries=int(os.environ.get("CURATED_RETRIES", "2")), base_delay=5)
        else:
            logger.info("Skipping curated step (RUN_CURATED is false)")

    except Exception as e:
        logger.exception("ETL pipeline failed: %s", str(e))
        exit_code = 2
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                logger.exception("Error stopping Spark")
        logger.info("ETL pipeline exiting with code %d", exit_code)
        sys.exit(exit_code)

if __name__ == "__main__":
    main()
