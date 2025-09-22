# Test Iceberg with Spark 3.5.x
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "memory") \
    .getOrCreate()

# Test Iceberg table creation
spark.sql("CREATE TABLE iceberg.test_table (id INT, name STRING) USING iceberg")
print("✅ Iceberg integration working with Spark 3.5.x")