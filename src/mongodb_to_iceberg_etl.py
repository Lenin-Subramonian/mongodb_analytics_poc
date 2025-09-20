# mongodb_to_iceberg_etl.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pymongo import MongoClient
import boto3
from datetime import datetime
import json

class MongoToIcebergETL:
    def __init__(self, mongo_uri, s3_bucket, aws_region='us-east-2'):
        self.mongo_uri = mongo_uri
        self.s3_bucket = s3_bucket
        self.aws_region = aws_region
        self.spark = self._create_spark_session()
    
    def _create_spark_session(self):
        """Create Spark session with Iceberg support"""
        return SparkSession.builder \
            .appName("MongoDB-to-Iceberg-ETL") \
            .config("spark.jars.packages", 
                   "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2,"
                   "org.apache.hadoop:hadoop-aws:3.3.4,"
                   "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
            .config("spark.sql.extensions", 
                   "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.iceberg.type", "hadoop") \
            .config("spark.sql.catalog.iceberg.warehouse", f"s3a://{self.s3_bucket}/iceberg-warehouse") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()
    
    def extract_from_mongodb(self, database, collection, batch_size=1000):
        """Extract data from MongoDB using pymongo"""
        client = MongoClient(self.mongo_uri)
        db = client[database]
        coll = db[collection]
        
        # Get total count for progress tracking
        total_docs = coll.count_documents({})
        print(f"Total documents to process: {total_docs}")
        
        # Extract data in batches
        all_docs = []
        for i in range(0, total_docs, batch_size):
            batch = list(coll.find().skip(i).limit(batch_size))
            all_docs.extend(batch)
            print(f"Processed {min(i + batch_size, total_docs)}/{total_docs} documents")
        
        client.close()
        
        # Convert MongoDB documents to Spark-compatible format
        processed_docs = []
        for doc in all_docs:
            # Handle ObjectId and datetime fields
            processed_doc = self._process_mongodb_doc(doc)
            processed_docs.append(processed_doc)
        
        return processed_docs
    
    def _process_mongodb_doc(self, doc):
        """Process MongoDB document to handle special types"""
        processed = {}
        for key, value in doc.items():
            if key == '_id':
                processed['_id'] = str(value)  # Convert ObjectId to string
            elif isinstance(value, datetime):
                processed[key] = value.isoformat()
            elif isinstance(value, dict):
                processed[key] = json.dumps(value)  # Flatten nested objects
            else:
                processed[key] = value
        return processed
    
    def create_spark_dataframe(self, docs):
        """Create Spark DataFrame from MongoDB documents"""
        # Define schema for better performance and type safety
        schema = StructType([
            StructField("_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("customer_email", StringType(), True),
            StructField("product", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("price", DoubleType(), True),
            StructField("order_date", StringType(), True),
            StructField("status", StringType(), True),
            StructField("shipping_address", StringType(), True),
            StructField("metadata", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(docs, schema)
        
        # Convert string dates back to timestamps
        df = df.withColumn("order_date", to_timestamp(col("order_date")))
        df = df.withColumn("extraction_timestamp", current_timestamp())
        
        return df
    
    def transform_for_analytics(self, df):
        """Apply transformations for analytical use"""
        # Parse nested JSON fields
        df = df.withColumn("shipping_city", 
                          get_json_object(col("shipping_address"), "$.city"))
        df = df.withColumn("shipping_state", 
                          get_json_object(col("shipping_address"), "$.state"))
        df = df.withColumn("campaign_id", 
                          get_json_object(col("metadata"), "$.campaign_id"))
        
        # Add derived columns
        df = df.withColumn("total_amount", col("quantity") * col("price"))
        df = df.withColumn("order_year", year(col("order_date")))
        df = df.withColumn("order_month", month(col("order_date")))
        df = df.withColumn("order_day", dayofmonth(col("order_date")))
        
        # Data quality checks
        df = df.filter(col("quantity") > 0)
        df = df.filter(col("price") > 0)
        
        return df
    
    def write_to_iceberg(self, df, table_name):
        """Write DataFrame to Iceberg table"""
        # Create Iceberg table with partitioning
        df.writeTo(f"iceberg.{table_name}") \
          .partitionedBy("order_year", "order_month") \
          .option("write.format.default", "parquet") \
          .option("write.parquet.compression-codec", "snappy") \
          .createOrReplace()
        
        print(f"Data written to Iceberg table: {table_name}")
    
    def run_etl_pipeline(self, database, collection, table_name):
        """Run the complete ETL pipeline"""
        print("Starting MongoDB to Iceberg ETL pipeline...")
        
        # Step 1: Extract
        print("Step 1: Extracting data from MongoDB...")
        docs = self.extract_from_mongodb(database, collection)
        
        # Step 2: Create DataFrame
        print("Step 2: Creating Spark DataFrame...")
        df = self.create_spark_dataframe(docs)
        
        # Step 3: Transform
        print("Step 3: Applying transformations...")
        transformed_df = self.transform_for_analytics(df)
        
        # Step 4: Load to Iceberg
        print("Step 4: Writing to Iceberg...")
        self.write_to_iceberg(transformed_df, table_name)
        
        # Display sample data
        print("Sample of transformed data:")
        transformed_df.show(5, truncate=False)
        
        # Data quality metrics
        total_records = transformed_df.count()
        print(f"Total records processed: {total_records}")
        
        print("ETL pipeline completed successfully!")
        
        return transformed_df

# Usage example
if __name__ == "__main__":
    # Configuration
    MONGO_URI = "mongodb+srv://ls_db_user:s47tBnWikllAoe3k@democluster0.j777gry.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster0"
    S3_BUCKET = "s3://fq-app-analytics-bucket-1/iceberg-warehouse/ecommerce_db_raw/"
    
    # Initialize ETL
    etl = MongoToIcebergETL(MONGO_URI, S3_BUCKET)
    
    # Run pipeline
    result_df = etl.run_etl_pipeline("ecommerce_db", "orders", "orders_analytics")
    
    # Stop Spark session
    etl.spark.stop()