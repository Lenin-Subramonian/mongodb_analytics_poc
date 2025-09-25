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

The solution uses PySpark (3.5.3), the Iceberg Spark runtime, and runs inside Docker containers orchestrated with Docker Compose.

## 🏗️ Architecture

*MongoDB → PySpark ETL → S3 (Apache Iceberg) → Query Engines → Analytics & BI*

 **Source**: Data is extracted from a MongoDB Atlas database (`FQ_App`) with collections:
   - `accounts`
   - `journal_entries`
   - `close_tasks`
 
 **Transformation**: Data is ingested into **Apache Iceberg tables** using **PySpark 3.5.3**.

This approach allows for:

- **Separation of concerns**: `raw` vs `curated` layers in the Iceberg warehouse.
- **Schema evolution**: Iceberg handles column addition/removal.
- **Time travel queries**: Analyze data at historical snapshots.
- **Decoupled compute/storage**: Data in S3 is accessible to multiple engines (Spark, Trino, Athena, Databricks).
- **Data Lakehouse** - a reliable, flexible, and scalable "lakehouse" architecture.
     - Combining the flexibility of data lakes with the structure and governance capabilities of data warehouses
     - Using Iceberg, a data lake moves beyond being just a collection of files to a data lakehouse.
     - the data stored in the lake can be organized, managed, and queried like a traditional database/data warehouse, but with the scalability and cost-effectiveness of a data lake. 

## Data Architecture

###  **Storage**
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


### **Catalog**
     
  - AWS Glue Catalog to manages table and schema definitions & provides a central metadata store.
  - The catalog keeps track of the current metadata file for each table. It is the entry point for all queries and operations on an Iceberg table.
  - Catalogs can be implemented using services like the Hive Metastore, AWS Glue Catalog, Databricks Unity Catalog or cloud-native options.
  - Enables external tools (e.g., Spark SQL, Databricks or Athena) to query Iceberg tables.


         - catalog = logical metastore, warehouse = shared S3 root,
         - namespaces/databases = raw/curated,
         - fq_app is either a sub-namespace or part of table name (s3://fq-app-analytics-bucket-1/iceberg-warehouse/raw/fq_app/)
    
           **Sample:** 
            ├── iceberg-warehouse/
            │   ├── fq_app_db_raw/       (Glue Database/Iceberg Namespace)
            │   │   ├── accounts/        (Iceberg Table for the orders collection)
            │   │   │   ├── metadata/    (Iceberg keeps metadata under table dir normally)
            │   │   │   ├── data/        (Iceberg keeps data under table dir normally)
         

      - **Warehouse root**: `s3://<bucket>/iceberg-warehouse/`
      - **Raw layer**: `s3://<bucket>/iceberg-warehouse/raw/<app_name>/<collection>`
      - **Curated layer**: `s3://<bucket>/iceberg-warehouse/curated/<app_name>/<table>`

**Catalog Naming**:

- **Catalog name**: `iceberg` (backed by AWS GlueCatalog)
     - logical catalog name (e.g., iceberg_catalog or glue_catalog)
- **Namespaces**: Simlar to databases, raw v/s curated or by application, for eg: fq_app_raw
     - `raw` → direct MongoDB extracts.
     - `curated` → transformed, analytics-ready datasets.
- **Tables**: one per collection: orders, customers.

**Query Engine**
      Query Engine Integration

 **Data Infrastructure**
 
  - Setup spark 3.5.3 runtime 'apache/spark:3.5.3-java17-python3' as the base image to process data using PySpark, Python and SQL.
  - Use Jupyter Notebook data analysis
  - MongoDB Atlas → source database with collections (accounts, close_tasks, journal_entries).

**Data Procesing**: 

  1. **Data Ingestion** - Raw Ingestion Layer
     
     -  Extracts collections (e.g., accounts, close_tasks, journal_entries) from MongoDB Atlas 
     -  Writes directly into S3 in Iceberg format (raw namespace).
     -  Schema in this layer matches MongoDB exactly, preserving source fidelity.
     -  Raw Layer → schema-preserving ingestion into Iceberg (S3 + Glue). Partitioned by ingest_ts.

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

🎯 Performance Optimization

MongoDB Optimization

Create indexes on query fields
Use projection to limit fields
Implement connection pooling
Consider read preferences for replica sets

Spark Optimization

Tune executor memory and cores
Optimize partition sizes (128MB-1GB)
Use appropriate file formats (Parquet)
Enable predicate pushdown

Iceberg Optimization

Choose appropriate partitioning strategy
Configure file sizes and compaction
Use columnar formats for analytics
Implement data compaction schedules

Query Engine Optimization

Trino: Tune memory and worker nodes
Spark: Optimize join strategies and caching
Athena: Use partitioning and columnar formats
General: Implement query result caching


🚀 Deployment Options

📋 Prerequisites
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

🚀 Features

Scalable Data Extraction: Efficient MongoDB data extraction using PyMongo and PySpark
Modern Data Lake: Apache Iceberg format with ACID transactions and schema evolution
Multiple Query Engines: Support for Trino, Spark SQL, Amazon Athena, and Dremio
Incremental Processing: Delta load capabilities for efficient data updates
Data Quality: Built-in data validation and quality checks
Production Ready: Docker containerization and Airflow orchestration

  ## Getting Started
  
   ### Dependencies
  - Ubuntu 24.04.1 LTS or Ensure WSL 2 (Windows Subsystem for Linux)
  - Java openjdk-11-jdk and set JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64
  - Set PYTHONPATH=$(pwd)/src:$PYTHONPATH (to execute the /src files.
### Glue / Iceberg Configuration
- Use **GlueCatalog** for central schema management.
- Set `spark.sql.catalog.iceberg.warehouse=s3://<bucket>/iceberg-warehouse/`.
- Ensure AWS credentials and region are available (via env vars or IAM roles).
- Include required AWS SDK v2 jars: `s3`, `glue`, `dynamodb`, `kms`.
      
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

 # Jupyter dependencies
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


Running Locally (with Docker Compose)

Configure environment variables in .env:

AWS_ACCESS_KEY_ID=xxxx
AWS_SECRET_ACCESS_KEY=xxxx
AWS_REGION=us-east-1
MONGO_URI=mongodb+srv://...
S3_BUCKET=fq-app-analytics-bucket-1

  ## Components

### 1. PySpark ETL Script (`mongodb_to_iceberg_etl.py`)

- Extracts MongoDB collections using `pymongo`.
- Converts documents to Spark DataFrames.
- Writes DataFrames to Iceberg tables (`CREATE TABLE IF NOT EXISTS ...`).
- Partitions data where appropriate (e.g., `orders` by `order_date`).

## Best Practices


Example log during write:
  
  ## How to install & Execute

  ### *Using Docker Image from Registry*
  
  1. **Install Docker on Windows**
       - Install Docker Desktop for Windows.
       - Ensure WSL 2 (Windows Subsystem for Linux) is enabled, as it's recommended for running Linux containers.
  2. **Pull the Image from a Registry**
      Docker image link [Docker Hub Link](https://hub.docker.com/repository/docker/mooney042/ifco-data-app/tags/latest/sha256-c978024b1b69ac3bb8636243c33646879eb9b025a61d1efc1fa8ecdfec7c8123)
     
      ```
      docker pull mooney042/ifco-data-app:latest
      ```
  3. **Run the Image on Windows**
      If the image is a Linux-based image, it will run fine on Windows if Docker is set to run Linux containers (which is the default with WSL 2).
      ```
      docker run -it --rm -p 8888:8888 -p 8501:8501 mooney042/ifco-data-app:latest
      ```
   4. **Accessing Notebook and Visualization**
        - **To access Jupyter and Streamlit by navigating to:**
            - Jupyter Notebook: http://localhost:8888
                - [Jupyter Notebook](http://localhost:8888/lab/tree/IFCO_Data_Analysis.ipynb)
            - Streamlit Dashboard: http://localhost:8501
  
  ### *Building a Docker image from Git*

1. **Install Docker on Windows**

   - Install Docker Desktop for Windows.
   - Ensure WSL 2 (Windows Subsystem for Linux) is enabled, as it's recommended for running Linux containers.
   
3. **Checkout or downaload all the files form the Git to your local project folder. Navigate to the project root folder in the terminal.** 
  
4. **Excute docker build process** 
     - If the Dockerfile is in a different directory than the current project directory, you need to specify its location using the -f flag.
     - Docker is looking for a Dockerfile in the current directory (.), but in the example below Dockerfile is inside the subdirectory Docker/ folder.
     
      ```
      docker build -t <my_container_name> -f Docker/Dockerfile .
      ```
5. **Run the Image**
      If the image is a Linux-based image, it will run fine on Windows if Docker is set to run Linux containers (which is the default with WSL 2).
      ```
      docker run -it --rm -p 8888:8888 -p 8501:8501 <my_container_name> 
      ```
7. **Accessing Notebook and Visualization**
     - **To access Jupyter and Streamlit by navigating to:**
     - Jupyter Notebook: http://localhost:8888
       -  [Jupyter Notebook](http://localhost:8888/lab/tree/IFCO_Data_Analysis.ipynb)
     -  Streamlit Dashboard: http://localhost:8501


## Assumptions
  - For the current application requirement, the source data both orders and invoices are static files.
  - No file format changes expected for the source filesv- the `orders` data is CSV format, `invoices` data is in JSON format.
  - No schema changes expected for both source data files.
  - No requirement to perisist the data for future use. 
  - Using Windows and enabling WSL 2 for running docker.

## How to Debug?
  1. Look for tracebacks or errors in the output. 
       ``` docker logs <container_id> ```

  2. Restart the container:

      ```
      docker stop <container_id> && docker rm <container_id>
      docker run -d <your-image>
      ```
  3. Verify Running Processes Inside the Container

      - Open an interactive shell inside the container:
          ```
          # get container id
          docker ps
          ```
          ```
          docker exec -it <container_id> /bin/bash
          ```
      - Manually Start Jupyter & Streamlit

          ```
          jupyter notebook --allow-root --ip=0.0.0.0 --port=8888
          ```
          ```
          streamlit run /path/to/app.py --server.port 8501 --server.address 0.0.0.0
          ```
     - Verify if supervisor is working in the container
           ```
           supervisorctl status
           supervisorctl restart all
            ```
## Improvements & Next Steps
  
  -  Ingesting the source data from realtime API or from a database or an automated process for production use.
  -  Persiting the data in database for reuability and making data available across teams and geographies.
  -  Use `Docker-Compose.yml` to deal with persisting data, multiple services, shared volumes, environment variables, or networking.
  -  Use data visulization tool such as Tableau for more dashboard capabilities as data is persisted in database.
  -  Change the base image to optimized Databricks image for better performance.
  -  Create purpose built dimension data models such as facts and dimensions or denormalised tables for better query and report performance.

## Reference Links

  1. [Dockerfile](https://github.com/Lenin-Subramonian/data-engineering-ifco_test/blob/main/Docker/Dockerfile)
  2. [Jupyter Notebook Git Link].(https://github.com/Lenin-Subramonian/data-engineering-ifco_test/blob/main/notebooks/IFCO_Data_Analysis.ipynb)



What this Dockerfile does:

Base Image: Uses OpenJDK 17 (compatible with Spark 3.5.3)
System Setup: Installs Python 3 and essential tools
Spark Installation: Downloads and configures Spark 3.5.3
Dependencies: Installs Python packages and downloads JAR files
Security: Creates non-root user for running applications
Health Check: Monitors Spark UI for container health


Build and run:

docker-compose up --build


Run ETL:

Service mongodb-iceberg-etl runs once and exits after writing data.

Check logs: docker logs mongodb-iceberg-etl

Explore with Jupyter:

Open Jupyter notebook (http://localhost:8888).

Run SQL queries against Iceberg tables in S





🔎 Explanation of Flow:


Analytics Tools → Spark SQL, Athena, Quicksight, Databricks, etc. query curated datasets.
