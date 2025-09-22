# scripts/download_jars.py - Download required JAR files for Spark 3.5.3
import requests
import os
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_jar(url, filename, target_dir):
    """Download JAR file if it doesn't exist"""
    target_path = Path(target_dir) / filename
    
    if target_path.exists():
        logger.info(f"✓ {filename} already exists")
        return True
    
    logger.info(f"Downloading {filename}...")
    try:
        response = requests.get(url, timeout=300)  # 5 minute timeout
        response.raise_for_status()
        
        # Ensure directory exists
        target_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(target_path, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"✓ Downloaded {filename} ({len(response.content)} bytes)")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to download {filename}: {e}")
        return False

def main():
    """Download all required JAR files for Spark 3.5.3"""
    
    # Create jars directory
    jars_dir = Path("/app/jars")
    jars_dir.mkdir(parents=True, exist_ok=True)
    
    # Required JAR files for Spark 3.5.3 with Iceberg + MongoDB + AWS
    jars = [
        {
            "name": "Iceberg Spark Runtime",
            "url": "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar",
            "filename": "iceberg-spark-runtime-3.5_2.12-1.5.2.jar"
        },
        {
            "name": "Hadoop AWS",
            "url": "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar",
            "filename": "hadoop-aws-3.3.6.jar"
        },
        {
            "name": "AWS Java SDK Bundle",
            "url": "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.540/aws-java-sdk-bundle-1.12.540.jar",
            "filename": "aws-java-sdk-bundle-1.12.540.jar"
        },
        {
            "name": "MongoDB Spark Connector",
            "url": "https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar",
            "filename": "mongo-spark-connector_2.12-10.3.0.jar"
        }
    ]
    
    logger.info("📦 Downloading JAR files for Spark 3.5.3...")
    logger.info(f"Target directory: {jars_dir}")
    
    success_count = 0
    total_count = len(jars)
    
    for jar_info in jars:
        success = download_jar(jar_info["url"], jar_info["filename"], jars_dir)
        if success:
            success_count += 1
    
    logger.info(f"\n✅ JAR download completed: {success_count}/{total_count} successful")
    
    if success_count == total_count:
        logger.info("🎉 All JAR files downloaded successfully!")
        
        # List downloaded files
        jar_files = list(jars_dir.glob("*.jar"))
        total_size = sum(f.stat().st_size for f in jar_files)
        logger.info(f"Total files: {len(jar_files)}")
        logger.info(f"Total size: {total_size / (1024*1024):.1f} MB")
        
        return True
    else:
        logger.error("❌ Some JAR files failed to download")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)