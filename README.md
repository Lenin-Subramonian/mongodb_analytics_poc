Build an end-to-end pipeline that:
1. Extracts accounting data from MongoDB
2. Writes to S3 in Apache Iceberg format
3. Transforms it for analytical use
4. Enables analysis via a query engine



What this Dockerfile does:

Base Image: Uses OpenJDK 17 (compatible with Spark 3.5.3)
System Setup: Installs Python 3 and essential tools
Spark Installation: Downloads and configures Spark 3.5.3
Dependencies: Installs Python packages and downloads JAR files
Security: Creates non-root user for running applications
Health Check: Monitors Spark UI for container health

# High-level architecture & goals -->

Goal: reliably land raw JSON-like MongoDB collections (orders, customers, products) into S3 as Iceberg tables (transactional, schema-evolving), keep a raw immutable zone, and build derived/processed tables for analytics.

Components:

Extractor: PySpark job (Spark 3.5.3) that reads from MongoDB (either via pymongo or MongoDB Spark Connector) and writes Iceberg tables to S3.

Data lake storage: S3 buckets, Iceberg-managed table directories.

Catalog: either AWS Glue Data Catalog (GlueCatalog) or Databricks Unity Catalog — both register table metadata for query engines.

Query engines: Spark (on EMR/Glue/Databricks), Trino/Presto, Athena (with Iceberg support), Redshift Spectrum (if needed). For this POC focus on Spark and Glue/Databricks. Iceberg runtime jar is required.

High-level flow:

Extract raw documents from MongoDB ecommerce_db_raw collections into a Spark DataFrame (structs/arrays handled).

Write to S3 as Iceberg-managed table in a raw namespace (immutable append-only ingestion).

Later runs can write incremental changes or create processed/analytics tables (CTAS, MERGE INTO) from the raw iceberg tables.


S3 directory mapping (best practice)

Map logical namespace → S3 prefix under the warehouse root:

s3://fq-app-analytics-bucket-1/iceberg-warehouse/
  ├─ raw/
  │   └─ fq_app/
  │       ├─ orders/         <-- iceberg table location
  │       ├─ customers/
  │       └─ products/
  ├─ curated/
  │   └─ fq_app/
  │       ├─ orders_silver/
  │       └─ customers_silver/
  └─ shared/                 # cross-app shared datasets


Why: keeping raw/curated at top-level simplifies lifecycle policies (e.g., retention on raw snapshots vs curated), IAM prefixes, and Glue database/table listing.

able naming examples

Raw tables (append-only, immutable):

iceberg.raw.fq_app_orders → physical dir .../raw/fq_app/orders/

iceberg.raw.fq_app_customers

Curated tables (cleaned/enriched):

iceberg.curated.fq_app_orders_v1 or iceberg.curated.fq_app_orders_silver

Shared datasets:

iceberg.curated.shared.dim_products

Recommended final PACKAGES for Glue (copy/paste)
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,
org.apache.hadoop:hadoop-aws:3.3.6,
software.amazon.awssdk:glue:2.20.137,
software.amazon.awssdk:s3:2.20.137,
software.amazon.awssdk:sts:2.20.137,
software.amazon.awssdk:dynamodb:2.20.137,
software.amazon.awssdk:kms:2.20.137,
org.mongodb.spark:mongo-spark-connector_2.12:10.3.0


s3://fq-app-analytics-bucket-1/iceberg-warehouse/raw.db/ecommerce_db_orders/


s3://fq-app-analytics-bucket-1/iceberg-warehouse/
  raw.db/                        # Hive-style folder for 'raw' database
    fq_app_orders/               # table directory for fq_app orders
    fq_app_customers/
  curated.db/
    fq_app_orders_silver/
    otherapp_orders_silver/


# MongoDB to Apache Iceberg POC (AWS Glue / Databricks)

## Overview

This proof-of-concept (POC) demonstrates how to build a **data ingestion and analytics pipeline** where:

1. **Source**: Data is extracted from a MongoDB database (`ecommerce_db_raw`) with collections:
   - `orders`
   - `customers`
   - `products`
2. **Transformation**: Data is ingested into **Apache Iceberg tables** using **PySpark 3.5.3**.
3. **Storage**: Data is persisted in **Amazon S3** in Iceberg format (Parquet data files + metadata).
4. **Catalog**: AWS Glue Catalog (or Databricks Unity Catalog in enterprise setup) manages table and schema definitions.

This approach allows for:
- **Separation of concerns**: `raw` vs `curated` layers in the Iceberg warehouse.
- **Schema evolution**: Iceberg handles column addition/removal.
- **Time travel queries**: Analyze data at historical snapshots.
- **Decoupled compute/storage**: Data in S3 is accessible to multiple engines (Spark, Trino, Athena, Databricks).

---

## Architecture




---

## Best Practices

### Data Layout
- **Warehouse root**: `s3://<bucket>/iceberg-warehouse/`
- **Raw layer**: `s3://<bucket>/iceberg-warehouse/raw/<app_name>/<collection>`
- **Curated layer**: `s3://<bucket>/iceberg-warehouse/curated/<app_name>/<table>`

This ensures multiple apps can share the same warehouse (`raw/`, `curated/` separated logically).

### Catalog Naming
- **Catalog name**: `iceberg` (backed by AWS GlueCatalog).
- **Namespaces**:
  - `raw` → direct MongoDB extracts.
  - `curated` → transformed, analytics-ready datasets.

Example table identifiers:
- `iceberg.raw.ecommerce_db_orders`
- `iceberg.curated.sales_summary`

### Glue / Iceberg Configuration
- Use **GlueCatalog** for central schema management.
- Set `spark.sql.catalog.iceberg.warehouse=s3://<bucket>/iceberg-warehouse/`.
- Ensure AWS credentials and region are available (via env vars or IAM roles).
- Include required AWS SDK v2 jars: `s3`, `glue`, `dynamodb`, `kms`.

---

## Components

### 1. PySpark ETL Script (`mongodb_to_iceberg_etl.py`)

- Extracts MongoDB collections using `pymongo`.
- Converts documents to Spark DataFrames.
- Writes DataFrames to Iceberg tables (`CREATE TABLE IF NOT EXISTS ...`).
- Partitions data where appropriate (e.g., `orders` by `order_date`).

Example log during write:



### 2. Docker Compose

- **mongodb-iceberg-etl**: Runs ETL job (Spark + Iceberg + AWS SDK jars).
- **jupyter**: Interactive analysis, allows queries like:
  ```python
  spark.sql("SHOW CREATE TABLE iceberg.raw.ecommerce_db_orders").show(truncate=False)


Running Locally (with Docker Compose)

Configure environment variables in .env:

AWS_ACCESS_KEY_ID=xxxx
AWS_SECRET_ACCESS_KEY=xxxx
AWS_REGION=us-east-1
MONGO_URI=mongodb+srv://...
S3_BUCKET=fq-app-analytics-bucket-1


Build and run:

docker-compose up --build


Run ETL:

Service mongodb-iceberg-etl runs once and exits after writing data.

Check logs: docker logs mongodb-iceberg-etl

Explore with Jupyter:

Open Jupyter notebook (http://localhost:8888).

Run SQL queries against Iceberg tables in S