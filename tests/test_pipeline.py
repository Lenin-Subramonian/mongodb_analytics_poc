# scripts/test_pipeline.py - Complete pipeline testing
import sys
import logging
import os
from pathlib import Path

# Add src to path
sys.path.append('/app/src')

from config import config
from mongodb_to_iceberg_etl import DockerizedMongoToIcebergETL

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_configuration():
    """Test configuration and environment variables"""
    logger.info("=== Testing Configuration ===")
    
    try:
        config.validate()
        logger.info("✅ Configuration validation passed")
        
        logger.info(f"MongoDB Database: {config.MONGODB_DATABASE}")
        logger.info(f"S3 Bucket: {config.S3_BUCKET}")
        logger.info(f"AWS Region: {config.AWS_DEFAULT_REGION}")
        
        return True
    except Exception as e:
        logger.error(f"❌ Configuration test failed: {e}")
        return False

def test_mongodb_connection():
    """Test MongoDB Atlas connection"""
    logger.info("=== Testing MongoDB Connection ===")
    
    try:
        from pymongo import MongoClient
        
        client = MongoClient(config.mongodb_uri, serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        
        db = client[config.MONGODB_DATABASE]
        collection = db[config.MONGODB_COLLECTION]
        doc_count = collection.count_documents({})
        
        logger.info(f"✅ MongoDB connection successful")
        logger.info(f"Collection {config.MONGODB_COLLECTION} has {doc_count} documents")
        
        client.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ MongoDB connection failed: {e}")
        return False

def test_aws_s3_connection():
    """Test AWS S3 connection"""
    logger.info("=== Testing AWS S3 Connection ===")
    
    try:
        import boto3
        
        s3_client = boto3.client(
            's3',
            aws_access_key_id=config.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
            region_name=config.AWS_DEFAULT_REGION
        )
        
        # Test bucket access
        response = s3_client.head_bucket(Bucket=config.S3_BUCKET)
        logger.info(f"✅ S3 bucket '{config.S3_BUCKET}' is accessible")
        
        # List objects to test permissions
        response = s3_client.list_objects_v2(Bucket=config.S3_BUCKET, MaxKeys=1)
        logger.info("✅ S3 list permissions verified")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ AWS S3 connection failed: {e}")
        return False

def test_spark_session():
    """Test Spark session creation"""
    logger.info("=== Testing Spark Session ===")
    
    try:
        from pyspark.sql import SparkSession
        
        spark = SparkSession.builder \
            .appName("Docker-Test") \
            .master("local[2]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .getOrCreate()
        
        logger.info(f"✅ Spark {spark.version} session created successfully")
        
        # Test basic operations
        data = [(1, "test"), (2, "data")]
        df = spark.createDataFrame(data, ["id", "value"])
        count = df.count()
        
        logger.info(f"✅ Basic DataFrame operations work: {count} records")
        
        spark.stop()
        return True
        
    except Exception as e:
        logger.error(f"❌ Spark session test failed: {e}")
        return False

def test_full_pipeline():
    """Test the complete ETL pipeline with sample data"""
    logger.info("=== Testing Complete ETL Pipeline ===")
    
    try:
        etl = DockerizedMongoToIcebergETL()
        
        # Run pipeline with limited data
        result_df = etl.run_pipeline(limit=100)
        
        logger.info("✅ Complete pipeline test successful")
        return True
        
    except Exception as e:
        logger.error(f"❌ Pipeline test failed: {e}")
        return False

def main():
    """Run all tests"""
    logger.info("🧪 Starting Docker POC Test Suite")
    logger.info("=" * 50)
    
    tests = [
        ("Configuration", test_configuration),
        ("MongoDB Connection", test_mongodb_connection),
        ("AWS S3 Connection", test_aws_s3_connection),
        ("Spark Session", test_spark_session),
        ("Full Pipeline", test_full_pipeline)
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        try:
            results[test_name] = test_func()
        except Exception as e:
            logger.error(f"Test {test_name} crashed: {e}")
            results[test_name] = False
        
        logger.info("-" * 30)
    
    # Summary
    logger.info("🏁 Test Summary")
    logger.info("=" * 50)
    
    passed = sum(results.values())
    total = len(results)
    
    for test_name, passed_test in results.items():
        status = "✅ PASS" if passed_test else "❌ FAIL"
        logger.info(f"{test_name}: {status}")
    
    logger.info(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        logger.info("🎉 All tests passed! Your Docker POC is ready.")
        sys.exit(0)
    else:
        logger.error("❌ Some tests failed. Check the logs above.")
        sys.exit(1)

if __name__ == "__main__":
    main()