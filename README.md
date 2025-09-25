# MongoDB → Iceberg Data Lakehouse Pipeline

## Problem Statement

Background:
You are working for an accounting SaaS platform focused on automating and tracking the month-end close process, journal entries, and account reconciliations. Your team uses MongoDB as the primary data store for accounting records, workflow tasks, and audit trails.
Objective:
Build an end-to-end pipeline that:
1. Extracts accounting data from MongoDB
2. Writes to S3 in Apache Iceberg format
3. Transforms it for analytical use
4. Enables analysis via a query engine

## Highlevel Solution & Approach

# MongoDB to Apache Iceberg POC (AWS Glue / Databricks)



This proof-of-concept (POC) demonstrates an end-to-end data pipeline that ingests raw data from MongoDB Atlas into Apache Iceberg tables (stored on S3, cataloged with AWS Glue), and builds curated (silver/gold) datasets for downstream analytics.

The solution uses PySpark (3.5.3), the Iceberg Spark runtime, and runs inside Docker containers orchestrated with Docker Compose.

🏗️ **Architecture**


**Data Architecture**

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

 1. **Data Infrastructure**
        - Setup databricks runtime as the base image to process data using PySpark, Python and SQL.
        - Use Jupyter Notebook and Streamlit for visualization.
        - Python and Java environment
        - Supervisor for process management

    **AWS Glue Catalog**

      Provides a central metadata store.

      Enables external tools (e.g., Athena, Spark SQL, Databricks) to query Iceberg tables.

     **Storage**



      **tooling**

      **Data Sources & Target**

**Data Procesing**

 
  2. **Data Ingestion** - Raw Ingestion Layer
     -  Extracts collections (e.g., accounts, close_tasks, journal_entries) from MongoDB. 
     -  Writes directly into S3 in Iceberg format (raw namespace).
     -  Schema in this layer matches MongoDB exactly, preserving source fidelity.


  3. **Data Transform** - Curated Layer (Silver / Gold)

      Transforms raw data into analytics-ready datasets:
      Silver: Deduplicated, cleaned, with schema evolution applied (e.g., date parsing, flattening nested structures).
      Gold: Aggregated metrics (e.g., journal entry debit/credit totals).
      Uses MERGE INTO for CDC / idempotent updates.
      Partitioned by business keys such as entry_date (year).

  5. **Data Analytics**
     - Generate datasets for each use case using the above views.
    
              -  Test 1: Distribution of Crate Type per Company
              -  Test 2: DataFrame of Orders with Full Name of the Contact
              -  Test 3: DataFrame of Orders with Contact Address
              -  Test 4: Calculation of Sales Team Commissions
              -  Test 5: DataFrame of Companies with Sales Owners
              -  Test 6: Data visualization data sets
      - Render the data visualization using Streamlit

  ## Getting Started
  
      ### Dependencies
        - Ubuntu 24.04.1 LTS or Ensure WSL 2 (Windows Subsystem for Linux)
        - Java openjdk-11-jdk and set JAVA_HOME=/usr/lib/jvm/java-1.11.0-openjdk-amd64
        - Set PYTHONPATH=$(pwd)/src:$PYTHONPATH (to execute the /src files. 
      
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


close_tasks_silver uses task_id or _id.

journal_entries_silver uses (entry_id, account_id) as the logical key for dedupe & MERGE.

journal_entries_gold uses entry_id.

+------------------------+-------+--------------+-----------+-----------+----------+--------------------------+
|_id                     |task_id|name          |assigned_to|status     |due_date  |ingest_ts                 |
+------------------------+-------+--------------+-----------+-----------+----------+--------------------------+
|68ca9b35193e5d257e046f25|T1     |Reconcile Cash|Alice      |Complete   |2025-06-05|2025-09-25 17:45:13.052109|
|68ca9b35193e5d257e046f26|T2     |Review AP     |Bob        |In Progress|2025-06-06|2025-09-25 17:45:13.052109|
+------------------------+-------+--------------+-----------+-----------+----------+--------------------------+

+------------------------+--------+----------+--------------+----------+----------------+------------+-----+------+-------------------------+
|_id                     |entry_id|entry_date|description   |account_id|account_name    |account_type|debit|credit|ingest_ts                |
+------------------------+--------+----------+--------------+----------+----------------+------------+-----+------+-------------------------+
|68ca9aca193e5d257e046f22|JE1001  |2025-05-31|Vendor payment|1000      |Cash            |Asset       |500.0|0.0   |2025-09-25 17:45:16.68436|
|68ca9aca193e5d257e046f22|JE1001  |2025-05-31|Vendor payment|2000      |Accounts Payable|Liability   |0.0  |500.0 |2025-09-25 17:45:16.68436|
+------------------------+--------+----------+--------------+----------+----------------+------------+-----+------+-------------------------+

+--------+----------+------------+-----------+
|entry_id|entry_date|total_credit|total_debit|
+--------+----------+------------+-----------+
|JE1001  |2025-05-31|500.0       |500.0      |
+--------+----------+------------+-----------+


+------------------------+--------+----------+--------------------------------+--------------+--------------------------+
|_id                     |entry_id|date      |lines                           |description   |ingest_ts                 |
+------------------------+--------+----------+--------------------------------+--------------+--------------------------+
|68ca9aca193e5d257e046f22|JE1001  |2025-05-31|[{1000, 500, 0}, {2000, 0, 500}]|Vendor payment|2025-09-23 16:46:52.534674|
+------------------------+--------+----------+--------------------------------+--------------+--------------------------+


+------------------------+----------+----------------+---------+--------------------------+
|_id                     |account_id|name            |type     |ingest_ts                 |
+------------------------+----------+----------------+---------+--------------------------+
|68ca9a71193e5d257e046f1c|1000      |Cash            |Asset    |2025-09-23 16:59:25.26308 |
|68ca9a71193e5d257e046f1d|2000      |Accounts Payable|Liability|2025-09-23 16:59:25.26308 |
+------------------------+----------+----------------+---------+--------------------------+



+------------------------+-------+--------------+-----------+-----------+----------+--------------------------+
|_id                     |task_id|name          |assigned_to|status     |due_date  |ingest_ts                 |
+------------------------+-------+--------------+-----------+-----------+----------+--------------------------+
|68ca9b35193e5d257e046f26|T2     |Review AP     |Bob        |In Progress|2025-06-06|2025-09-23 04:49:08.584766|
|68ca9b35193e5d257e046f25|T1     |Reconcile Cash|Alice      |Complete   |2025-06-05|2025-09-23 04:49:08.584766|
+------------------------+-------+--------------+-----------+-----------+----------+--------------------------+



flowchart TD
    A[MongoDB Atlas\n(ecommerce_db_raw)] -->|Extract via PyMongo| B[Raw Layer\nIceberg Tables (S3, Glue)]
    
    subgraph Raw Layer
        B1[raw.fq_app_accounts]
        B2[raw.fq_app_close_tasks]
        B3[raw.fq_app_journal_entries]
    end

    B -->|Transform + Dedup + Flatten| C[Curated Layer\nSilver Tables]

    subgraph Silver Layer
        C1[curated.fq_app_close_tasks_silver]
        C2[curated.fq_app_journal_entries_silver]
    end

    C -->|Aggregations & Metrics| D[Curated Layer\nGold Tables]

    subgraph Gold Layer
        D1[curated.fq_app_journal_entries_gold]
    end

    D -->|Query with Spark SQL, Athena, Databricks| E[Analytics / BI Tools]


🔎 Explanation of Flow:

MongoDB Atlas → source database with collections (accounts, close_tasks, journal_entries).

Raw Layer → schema-preserving ingestion into Iceberg (S3 + Glue). Partitioned by ingest_ts.

Silver Layer → cleaned, deduplicated data with flattened structures and business partitions (e.g., year(entry_date)).

Gold Layer → aggregated metrics tables for analytics (e.g., debit/credit totals).

Analytics Tools → Spark SQL, Athena, Quicksight, Databricks, etc. query curated datasets.