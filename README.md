Build an end-to-end pipeline that:
1. Extracts accounting data from MongoDB
2. Writes to S3 in Apache Iceberg format
3. Transforms it for analytical use
4. Enables analysis via a query engine


# IFCO Data Application
## Problem Statement

  To assist IFCO's Data Team in the analysis of some business data. For this purpose, you have been provided with two files:
  1. orders.csv (which contains facmtual information regarding the orders received)
  2. invoicing_data.json (which contains invoicing information)
    
  For this exercise, you can only use Python, PySpark or SQL (e.g. in dbt). Unit testing is essential for ensuring the reliability and correctness of your code. 
  Please include appropriate unit tests for each task.

## Highlevel Solution & Approach
  1. **Data Infrastructure**
        - Setup databricks runtime as the base image to process data using PySpark, Python and SQL.
        - Use Jupyter Notebook and Streamlit for visualization.
        - Python and Java environment
        - Supervisor for process management
  2. **Data Ingestion**.
     -  Ingest orders data (CSV) into a dataframe.Clean the source data for ingestion such as removing special characters etc. 
     -  Ingest invoices data (JSON) into a data frame, and create a view, invoices.
     -  Create base views `orders` and `Invoices`
  3. **Data Transform**
     - Transform the data, apply data validation rules and create views for reuse.  
       - `ORDERS_VW` - Create base view with business rules. 
       - `SALESOWNERS_VW` - Create a de-normalized view of sales owners.
       - `SALES_OWNER_COMMISSION_VW` - Create a salesowners commission View
       - `ORDER_SALES_OWNER_VW` - Create a de-normalised view of order and sales owners - a record per order & sales owner.
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
        - pandas
        - pyspark
        - urllib3
        - streamlit
          
      **Jupyter dependencies**
        - notebook
        - jupyterlab
        - ipykernel
        - ipywidgets
        - matplotlib
      
      **Additional dependencies**
        - requests

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