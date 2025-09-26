# MongoDB → Iceberg Data Lakehouse Pipeline

## Problem Statement

**Background:**

You are working for an accounting SaaS platform focused on automating and tracking the month-end close process, journal entries, and account reconciliations. Your team uses MongoDB as the primary data store for accounting records, workflow tasks, and audit trails.
Objective:
   Build an end-to-end pipeline that:
   1. Extracts accounting data from MongoDB
   2. Writes to S3 in Apache Iceberg format
   3. Transforms it for analytical use
   4. Enables analysis via a query engine

## Highlevel Solution & Approach

This proof-of-concept (POC) demonstrates an end-to-end data pipeline that ingests raw data from MongoDB Atlas into Apache Iceberg tables (stored on S3, cataloged with AWS Glue), and builds a curated (silver/gold) datasets for downstream analytics.

The solution uses PySpark (3.5.3) & Pymongo, the Iceberg Spark runtime, and runs inside Docker containers orchestrated with Docker Compose.

## 🏗️ Architecture

***MongoDB → PySpark/PyMongo ETL → S3 (Apache Iceberg) → Query Engines (Spark SQL) → Analytics & BI (Jupyter Notebook)***

 **Source**: Data is extracted from a MongoDB Atlas database (`FQ_App`) with collections on an on-demand basis. 
   - `accounts`
   - `journal_entries`
   - `close_tasks`
 
 **Destination/Sink**: Data is ingested into S3 with **Apache Iceberg tables** using **PySpark 3.5.3**.

 **Components**:
 
   -   Data Ingestion/Extraction: PySpark job (Spark 3.5.3) that reads from MongoDB (either via pymongo or MongoDB Spark Connector) and writes Iceberg tables to S3.
   
   -   Data storage: S3 buckets, Iceberg-managed table directories.
   
   -   Catalog: either AWS Glue Data Catalog (GlueCatalog) or Databricks Unity Catalog — both register table metadata for query engines.
   
   -   Data Processing & Query engines: Spark (on EMR/Glue/Databricks), Trino/Presto, Athena (with Iceberg support), Redshift Spectrum (if needed). For this POC focus on Spark and Glue/Databricks. Iceberg runtime jar is required.

### 🚀 Features

- **Scalable Data Extraction**: Efficient MongoDB data extraction using PyMongo and PySpark
- **Multiple Query Engines**: Cross platform interoprability, and support for Trino, Spark SQL, Databricks, Amazon Athena etc. 
- **Incremental Processing**: Delta load capabilities for efficient data updates
- **Modular & Separation of concerns**: `raw` vs `curated` layers in the Iceberg warehouse.
- **Modern Data Lakehouse** - Apache Iceberg format with ACID transactions and schema evolution
  
        - Time travel queries and scalable "lakehouse" architecture.
        - Combining the flexibility of data lakes with the structure and governance capabilities of data warehouses
        - Using Iceberg, a data lake moves beyond being just a collection of files to a data lakehouse.
        - the data stored in the lake can be organized, managed, and queried like a traditional database/data warehouse, but with the scalability and cost-effectiveness of a data lake.

## ✅ Data Architecture 

   ### 📂 **Data Storage**
   
   - Data is persisted in **Amazon S3** in Iceberg format (Parquet data files + metadata).

     **S3 layout & Iceberg warehouse structure**
     
         - Use a single warehouse root per catalog; within it use database (namespace) / table directories:
         - set spark.sql.catalog.<catalog>.warehouse to s3a://<bucket>/warehouse/'

         s3://<bucket>/warehouse/
           ├─ raw/                      # raw immutable landing zone
           │   ├─ fq_app/
           │   │   ├─ accounts/         # Iceberg table dir for raw.accounts
           │   │   ├─ journal_entries/
           │   │   └─ close_tasks/
           ├─ curated/                   # processed / analytics-ready tables
           │   ├─ fq_app/
           │   │   ├─ close_tasks_silver/
           │   │   └─ journal_entries_silver/


   ### 📚 **Catalog**
     
  - AWS Glue Catalog to manages table and schema definitions & provides a central metadata store.
  - The catalog keeps track of the current metadata file for each table. It is the entry point for all queries and operations on an Iceberg table.
  - Catalogs can be implemented using services like the Hive Metastore, AWS Glue Catalog, Databricks Unity Catalog or cloud-native options.
  - Enables external tools (e.g., Spark SQL, Databricks or Athena) to query Iceberg tables.

   **Catalog Naming**:
   
   - *Catalog name*: `iceberg` - logical catalog name (e.g., iceberg_catalog or glue_catalog)
   - *Warehouse root* s3a://<bucket>/iceberg-warehouse/
   - *Namespaces*: Simlar to databases, raw v/s curated or by application, for eg: fq_app_raw
        - `raw` → direct MongoDB extracts.
        - `curated` → transformed, analytics-ready datasets.
   - *App-level segregation*: create subfolders per application
		   - as part of the table name (e.g., raw.fq_app_orders → physical raw.db/fq_app_orders)
   - *Tables*: one per collection: accounts, customers.

         - catalog = logical metastore, warehouse = shared S3 root,
         - namespaces/databases = raw/curated,
         - application name, fq_app is either a sub-namespace or part of table name (s3://fq-app-analytics-bucket-1/iceberg-warehouse/raw/fq_app/)
    
           **Sample:** 
            ├── iceberg-warehouse/
            │   ├── fq_app_db_raw/       (Glue Database/Iceberg Namespace)
            │   │   ├── accounts/        (Iceberg Table for the orders collection)
            │   │   │   ├── metadata/    (Iceberg keeps metadata under table dir normally)
            │   │   │   ├── data/        (Iceberg keeps data under table dir normally)
         

         - **Warehouse root**: `s3://<bucket>/iceberg-warehouse/`
         - **Raw layer**: `s3://<bucket>/iceberg-warehouse/raw/<app_name>/<collection>`
         - **Curated layer**: `s3://<bucket>/iceberg-warehouse/curated/<app_name>/<table>`

   ### ✅ Data Infrastructure

   -   Setup spark 3.5.3 runtime 'apache/spark:3.5.3-java17-python3' as the base image to process data using PySpark, Python and SQL.
   -   Use Jupyter Notebook service for data analysis.
   -   MongoDB Atlas → source database with collections (accounts, close_tasks, journal_entries).
   -   S3 for data storage with Icebrg format and Parquet
   -   AWS Glue as catalog

   🐳 **Docker & Containerization**
   
   ***Why Containers?***
   
   Containers package the entire data pipeline (PySpark, Iceberg, Glue integration, PyMongo, dependencies) into reproducible, isolated environments. This ensures that ETL runs consistently across dev, test, and prod without "works on my machine" issues. 
   
   In this POC: 
   1.   mongodb-iceberg-etl container runs batch ETL → extracts from MongoDB, writes to Iceberg (S3 + Glue).
   2.   jupyter-analytics container allows interactive exploration of Iceberg tables with Spark SQL.
   3.   Data persisted in S3 outlives container restarts, while containers themselves are stateless and ephemeral.

   ***Docker***

   Each service (e.g., mongodb-iceberg-etl, jupyter-analytics) runs inside its own Docker image. Initiates custom ETL code (mongo_ingestion_runner.py).
      - Base Image: Uses OpenJDK 17 (compatible with Spark 3.5.3)
      - System Setup: Installs Python 3 and essential tools
      - Spark Installation: Downloads and configures Spark 3.5.3
      - Security: Creates non-root user for running applications
      - Health Check: Monitors Spark UI for container health
      - Dependencies: Installs Python packages and downloads JAR files
     
            org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2 (Iceberg runtime for Spark 3.5)
            org.apache.hadoop:hadoop-aws:3.3.6 (S3A)
            software.amazon.awssdk:glue:2.20.137 (Glue SDK v2)
            software.amazon.awssdk:s3:2.20.137 (S3 SDK v2)
            software.amazon.awssdk:sts:2.20.137 (STS)
            software.amazon.awssdk:dynamodb:2.20.137 (DynamoDB)
            software.amazon.awssdk:kms:2.20.137 (KMS)
            org.mongodb.spark:mongo-spark-connector_2.12:10.3.0

   ***Docker Compose***

   -   Orchestrates multiple containers as a stack.
   -   Defines services like ETL job runner, Jupyter Notebook (for ad-hoc queries), and supporting dependencies.
   -   Handles networking between containers.
   -   Mounts config/env files and AWS credentials into containers for S3 + Glue access.
   -   Allows one command (docker-compose up --build) to start the full pipeline locally.
   -   Docker named volumes for Ivy/Maven cache so dependency downloads persist and avoid Windows bind-permission issues.


   ### ✅ Data Procesing

   1. **Data Ingestion** - Raw Ingestion Layer
     
      -   keep a raw immutable data with source schema. Schema in this layer matches MongoDB exactly, preserving source fidelity.
      -   Extracts collections (e.g., accounts, close_tasks, journal_entries) from MongoDB Atlas
      -   Writes directly into S3 in Iceberg format (raw namespace).
         -    Raw Layer → schema-preserving ingestion into Iceberg (S3 + Glue). Partitioned by ingest_ts.
           

            **journal_entries**:
            +------------------------+--------+----------+--------------------------------+--------------+--------------------------+
            |_id                     |entry_id|date      |lines                           |description   |ingest_ts                 |
            +------------------------+--------+----------+--------------------------------+--------------+--------------------------+
            |68ca9aca193e5d257e046f22|JE1001  |2025-05-31|[{1000, 500, 0}, {2000, 0, 500}]|Vendor payment|2025-09-23 16:46:52.534674|
            +------------------------+--------+----------+--------------------------------+--------------+--------------------------+

            **close_accounts**
            +------------------------+-------+--------------+-----------+-----------+----------+--------------------------+
            |_id                     |task_id|name          |assigned_to|status     |due_date  |ingest_ts                 |
            +------------------------+-------+--------------+-----------+-----------+----------+--------------------------+
            |68ca9b35193e5d257e046f26|T2     |Review AP     |Bob        |In Progress|2025-06-06|2025-09-23 04:49:08.584766|
            |68ca9b35193e5d257e046f25|T1     |Reconcile Cash|Alice      |Complete   |2025-06-05|2025-09-23 04:49:08.584766|
            +------------------------+-------+--------------+-----------+-----------+----------+--------------------------+

  2. **Data Transform** - Curated Layer (Silver / Gold)
     
     -   Transforms raw data into analytics-ready datasets
     -   Uses MERGE INTO for CDC / idempotent updates.
     -   Partitioned by business keys such as entry_date (year).

     **Silver Layer**
        → cleaned, deduplicated data with flattened structures and business partitions (e.g., year(entry_date)).
        → (e.g., date parsing, flattening nested structures).

        <img width="701" height="531" alt="image" src="https://github.com/user-attachments/assets/dbca920f-29e1-4c9b-9646-d113c2500158" />


         **close_tasks_silver**: 
         +------------------------+-------+--------------+-----------+-----------+----------+--------------------------+
         |_id                     |task_id|name          |assigned_to|status     |due_date  |ingest_ts                 |
         +------------------------+-------+--------------+-----------+-----------+----------+--------------------------+
         |68ca9b35193e5d257e046f25|T1     |Reconcile Cash|Alice      |Complete   |2025-06-05|2025-09-25 17:45:13.052109|
         |68ca9b35193e5d257e046f26|T2     |Review AP     |Bob        |In Progress|2025-06-06|2025-09-25 17:45:13.052109|
         +------------------------+-------+--------------+-----------+-----------+----------+--------------------------+

         **journal_entries_silver**: denormalised, joined data between accounts and journal entries
         +------------------------+--------+----------+--------------+----------+----------------+------------+-----+------+-------------------------+
         |_id                     |entry_id|entry_date|description   |account_id|account_name    |account_type|debit|credit|ingest_ts                |
         +------------------------+--------+----------+--------------+----------+----------------+------------+-----+------+-------------------------+
         |68ca9aca193e5d257e046f22|JE1001  |2025-05-31|Vendor payment|1000      |Cash            |Asset       |500.0|0.0   |2025-09-25 17:45:16.68436|
         |68ca9aca193e5d257e046f22|JE1001  |2025-05-31|Vendor payment|2000      |Accounts Payable|Liability   |0.0  |500.0 |2025-09-25 17:45:16.68436|
         +------------------------+--------+----------+--------------+----------+----------------+------------+-----+------+-------------------------+

      **Gold Layer** → aggregated metrics tables for analytics (e.g., debit/credit totals).

         **journal_entries_gold**:
         +--------+----------+------------+-----------+
         |entry_id|entry_date|total_credit|total_debit|
         +--------+----------+------------+-----------+
         |JE1001  |2025-05-31|500.0       |500.0      |
         +--------+----------+------------+-----------+

### 📊 Query Engine

For the POC, Apache Spark (Spark SQL) is used as the query engine which is an open source, powerful, in-memory, distributed analytics engine with Spark SQL for querying structured data. 

Other Query Engines considered: 

   | Engine        | Features                                 | Best for  |
   |---------------|-----------------------------------------------------------|----------------------------------------------------------------|
   | Apache Spark | Native Iceberg support, Handles large datasets, Rich transformation capabilities| Complex transformations, Batch processing, Spark Streaming |
   | Trino | Excellent Iceberg support, Fast interactive queries,Multi-catalog federation, Requires separate cluster, Memory intensive|Interactive analytics, Ad-hoc queries|
   | Databricks |  Native Iceberg support, Optimized Spark  | Built on Spark & Delta format, Complex transformations with AI & ML  |
   | Snowflake | IntegratedIceberg support, Snowflake Open Catalog Integration| Cloud-based data warehousing  |
   | Amazon Athena | Serverless, Pay-per-query, Iceberg support (v3)  | Limited customization, Cold start latency | Cost-effective querying, Occasional use|

### 🎯 Performance Optimization

**Spark Optimization**

   -   Tune executor memory and cores, Optimize partition sizes (128MB-1GB), Use appropriate file formats (Parquet), Enable predicate pushdown
   -   Query Engine Optimization -  Spark: Optimize join strategies and caching, Partitioning
     
**Iceberg Optimization**

   -   Choose appropriate partitioning strategy,Configure file sizes and compaction, Use columnar formats for analytics, Implement data compaction schedules.
           • Partition raw by ingest_date or days(date) if queries often filter by date. Avoid too many small partitions or very high-cardinality partition keys.
	        • Use Parquet (Iceberg default) with Snappy compression.
	-   Target file size ~256MB-1GB depending on workload; configure Iceberg table property write.target-file-size-bytes.
   -   Iceberg table properties for performance:

            TBLPROPERTIES (
              'write.target-file-size' = '536870912',
              'format-version' = '2',
              'write.metadata.previous-versions-max' = '5',
              'read.split.target-size' = '134217728'
            )

   -   Snapshot Management
           • reduces S3 storage costs, keeps metadata lean. Expired snapshots are no longer queryable, but the current table state remains intact.
           • Use periodic compaction / rewriteManifests / expireSnapshots jobs to keep metadata healthy
           • Expire snapshots: removes references to older snapshots so Iceberg no longer considers those versions part of the table’s history. This will make the older data files eligible for cleanup.
           • Remove orphan files / vacuum: detects data files that are no longer referenced by any active snapshot and deletes them from the object store (S3). This reclaims storage.
       
            Implemented with:
            spark.sql(f"CALL iceberg.system.expire_snapshots('{table_name}', TIMESTAMP '2024-01-01 00:00:00')")
            spark.sql(f"CALL iceberg.system.remove_orphan_files('{table_name}')")

       	• Recommended policy:
         		○ Keep a modest retention (e.g., retain last 3 snapshots, expire older than 30 days) for raw and silver tables.
         		○ Keep longer retention for gold or critical tables (e.g., 90+ days).
         		○ Schedule expiry during low-traffic windows.
         		○ Always test expiry on a non-production copy first.

### ⚙️ Components

   -   'mongodb_iceberg_fqapp.py' → Extracts from MongoDB and writes to raw.
     
               - Extracts MongoDB collections using `pymongo`.
               - Converts documents to Spark DataFrames.
               - Writes DataFrames to Iceberg tables (`CREATE TABLE IF NOT EXISTS ...`).
               - Partitions data where appropriate (e.g.by `ingest_ts`).
       
   -   'curated_silver_fqapp.py' → Builds curated silver/gold datasets.
   -   'mongo_ingestion_runner.py → Orchestrates raw ingestion with retries & error handling.
   -   'docker-compose.yml → Defines Spark + Jupyter containers for local execution.
   -   'Dockerfile & entrypoint.sh' - Container definition and dependency management.


### 🛠️ Design Considerations

   Raw Layer
   
      ✅ Immutable raw data, append only. Schema - source schema (MongoDB collections).
      ✅ Partitioned by ingest_ts for incremental loads.
      ✅ Idempotency: safe_create_or_append_df() handles existing tables.
   
   Curated Layer
   
      ✅ Deduplication applied (dropDuplicates()).
      ✅ Partitioned by business columns (e.g., year(entry_date)).
      ✅ CDC support via MERGE INTO.
   

  ## 🚀Getting Started

   ###  📋 Prerequisites

   Software Requirements

      Python 3.8+
      Apache Spark 3.4+
      MongoDB 4.4+
      AWS CLI configured
      Docker (optional)
      Apache Airflow (for orchestration)

   AWS Services
   
      S3 bucket for data storage
      IAM roles with appropriate permissions
      Athena (optional, for serverless querying)

      
   **Core dependencies**
   
        pyspark==3.5.3
        pymongo==4.8.0
        boto3==1.34.144
        pandas==2.1.4
        python-dotenv==1.0.1
        requests==2.31.0
        numpy==1.24.4
        seaborn==0.13.0
        py4j==0.10.9.7

 **Jupyter dependencies**
 
        jupyter==1.0.0
        jupyterlab==4.0.9
        ipykernel==6.29.0
        ipywidgets
        matplotlib==3.8.2
        plotly==5.17.0

  ### 2. Docker Compose

   - **mongodb-iceberg-etl**: Runs ETL job (Spark + Iceberg + AWS SDK jars).
   - **jupyter**: Interactive analysis, allows queries like:
     
     ```python
     spark.sql("SHOW CREATE TABLE iceberg.raw.ecommerce_db_orders").show(truncate=False)


### 🚀 Running the Pipeline

   1. Environment Setup
   
   Configure environment variables in .env:
   
		# Mongo Config
		MONGO_URI=mongodb+srv://ls_db_user:s47tBnWikllAoe3k@democluster0.j777gry.mongodb.net/
		MONGODB_ATLAS_USERNAME=ls_db_user
		MONGODB_ATLAS_PASSWORD=s47tBnWikllAoe3k
		MONGODB_ATLAS_CLUSTER=democluster0.j777gry.mongodb.net
		MONGODB_DATABASE=fq_app
		RAW_NAMESPACE=raw_data
		CURATED_NAMESPACE=curated
		
		# # AWS Configuration
		AWS_ACCESS_KEY_ID=xxxx
		AWS_SECRET_ACCESS_KEY=xxxx
		AWS_DEFAULT_REGION=us-east-2
		S3_BUCKET=fq-app-analytics-bucket-1
		GLUE_CATALOG_NAME=iceberg
		
		# Application Configuration
		LOG_LEVEL=INFO
		BATCH_SIZE=1000
		SPARK_DRIVER_MEMORY=2g
		SPARK_EXECUTOR_MEMORY=4g
		SPARK_EXECUTOR_CORES=2
		
		# Jupyter Configuration
		JUPYTER_TOKEN=xxxx

   2. Start Docker Stack

            docker compose up --build


## How to Debug?

   1. Classpath conflicts — if you have both AWS v1 and v2 jars on classpath (e.g. baked aws-java-sdk-bundle jar plus v2 clients), that can cause issues. Remove any v1 jars from /opt/spark/jars in the image (remove wget lines in Dockerfile that download v1 bundle) and rebuild.
   	
   2. Version mismatches — Iceberg expects certain AWS SDK behaviors. If you still see missing classes, try changing the AWS SDK v2 version (2.20.x family). Use Maven Central to pick a recent 2.x release. Maven Central+1
   
   3. Glue extensions — for some Glue interactions teams add glue-catalog-extensions-for-iceberg-runtime artifacts (AWS Labs) — generally not required for standard GlueCatalog use but useful for certain Glue setups. Only add if docs indicate.

   4. Check the docker logs:
      
             # Check container status
                  docker-compose ps

            # logs
               docker-compose logs -f mongodb-iceberg-etl
            
            # Stop the container
               docker-compose down
                 
            # Torestart conatiner
               docker-compose up -d


## 🧩 Improvements & Next Steps

   1.   Use Mongo Spark Connector intead of pymongo for massive collections.
         -   This POC uses pymongo and reads data through the driver, so it will not scale to very large collections. For massive collections use the Mongo Spark Connector or stream via Change Streams.
         -   MongoDB Spark Connector for parallel reads, structured streaming and scale — far more efficient than single-threaded pymongo scans.

   2.   CDC & Incremental loads - use change-stream ingestion for incremental updates.
	      • Preferred for low-latency:
              - MongoDB Change Streams → ingestion service (Kafka / Debezium or a streaming Spark job) that writes events to Iceberg using MERGE INTO into curated tables or append into raw changelog table.
              - If using Change Streams, ensure idempotency and ordering; Iceberg's transactional guarantees help for atomic commits.
	      • Batch Approach: nightly full/patched extracts using updated_at or modified_ts field: query Mongo for docs updated_at > last_run_ts, append to raw and then process.
      
   3. Change the base image to optimized Databricks image for better performance or Baking jars into the image (recommended for production): 
         -   Pros: deterministic startup, faster, avoid Ivy permission/network problems.
         -   Cons: larger image, need to manage jar versions.

   4. Maintain ingestion watermark / checkpointing
         -   Store last processed _id or timestamp in a small metadata table (S3 JSON, DynamoDB, or Glue table). On next run, only query Mongo for documents newer than that watermark.
               -   Pros: avoids reprocessing same docs, cheap, simple. On restart, load it and start from that _id.
               -   Example: store last processed ingest_ts or Mongo ObjectId (which is increasing). Use find({"_id": {"$gt": ObjectId(last_id)}}).
         -   Use change streams or CDC
               -   MongoDB change streams → Kafka → Spark provides near-real-time and incremental exactly-once semantics.

   5. Use query engine Databricks / Spark SQL
         -   Pros: great when you already use Databricks for ETL/ML; Databricks has Iceberg integrations and Unity Catalog; powerful for heavy ETL + ad-hoc analytics. Good for transformation workloads and for teams with Spark expertise. 
      
   6. Create purpose built dimension data models such as facts and dimensions or denormalised tables for better query and report performance. Use data visulization tool such as Tableau for more dashboard capabilities as data is persisted in database.
      
   7. Orchestration using Airflow and Dbt
      
   9. Production / Operational / Security considerations
         -   Use IAM roles (EC2/ECS/EKS) rather than long-lived keys
         -   Use partitioned Iceberg tables (e.g., days(date) or ingest_date) — demonstrated in comments,
         -   Use Glue Data Catalog config, KMS configs, and a robust retry/recovery pattern

## Reference 

A. Links

  1. Icenreg procedures for [Iceberg Spark Procedures](https://iceberg.apache.org/docs/1.10.0/docs/spark-procedures/?h=remove_orphan_files#usage_8)
  2. [Jupyter Notebook Git Link].(https://github.com/Lenin-Subramonian/mongodb_analytics_poc/blob/main/notebooks/MongoDB_Data_Analysis.ipynb)

B. Catalog configuration examples - Spark config (PySpark example building SparkSession):

      from pyspark.sql import SparkSession
      
      spark = SparkSession.builder \
        .appName("mongo-to-iceberg") \
        .config("spark.jars.packages",
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
                "org.apache.hadoop:hadoop-aws:3.3.6,"
                "com.amazonaws:aws-java-sdk-bundle:1.12.540,"
                "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
        # Register Glue catalog
        .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.glue_catalog.warehouse", "s3a://<bucket>/warehouse/") \
        .config("spark.sql.catalog.glue_catalog.cache-enabled", "true") \
        .config("spark.hadoop.fs.s3a.endpoint", "<s3-endpoint-if-minio>") \
        .getOrCreate()






