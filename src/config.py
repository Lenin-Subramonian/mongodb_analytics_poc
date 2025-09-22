# src/config.py - Configuration management for Docker environment
import os
from urllib.parse import quote_plus
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

class Config:
    """Configuration class for MongoDB Analytics POC"""
    
    # MongoDB Atlas Configuration
    MONGODB_ATLAS_USERNAME = os.getenv('MONGODB_ATLAS_USERNAME')
    MONGODB_ATLAS_PASSWORD = os.getenv('MONGODB_ATLAS_PASSWORD')  
    MONGODB_ATLAS_CLUSTER = os.getenv('MONGODB_ATLAS_CLUSTER')
    MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'ecommerce_db')
    MONGODB_COLLECTION = os.getenv('MONGODB_COLLECTION', 'orders')
    
    # AWS Configuration
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_DEFAULT_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-2')
    S3_BUCKET = os.getenv('S3_BUCKET')
    
    # Spark Configuration
    SPARK_DRIVER_MEMORY = os.getenv('SPARK_DRIVER_MEMORY', '2g')
    SPARK_EXECUTOR_MEMORY = os.getenv('SPARK_EXECUTOR_MEMORY', '4g')
    SPARK_EXECUTOR_CORES = os.getenv('SPARK_EXECUTOR_CORES', '2')
    
    # Application Configuration
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    BATCH_SIZE = int(os.getenv('BATCH_SIZE', 1000))
    
    @property
    def mongodb_uri(self):
        """Get MongoDB connection URI"""
        if all([self.MONGODB_ATLAS_USERNAME, self.MONGODB_ATLAS_PASSWORD, self.MONGODB_ATLAS_CLUSTER]):
            username = quote_plus(self.MONGODB_ATLAS_USERNAME)
            password = quote_plus(self.MONGODB_ATLAS_PASSWORD)
            return f"mongodb+srv://{username}:{password}@{self.MONGODB_ATLAS_CLUSTER}/{self.MONGODB_DATABASE}?retryWrites=true&w=majority"
        else:
            # Fallback to local MongoDB in Docker
            return os.getenv('LOCAL_MONGO_URI', 'mongodb://mongodb:27017/')
    
    @property
    def s3_warehouse_path(self):
        """Get S3 warehouse path for Iceberg"""
        return f"s3a://{self.S3_BUCKET}/iceberg-warehouse/{self.MONGODB_DATABASE}_raw/"
    
    def validate(self):
        """Validate configuration"""
        errors = []
        
        if not self.S3_BUCKET:
            errors.append("S3_BUCKET is required")
        
        if not self.AWS_ACCESS_KEY_ID:
            errors.append("AWS_ACCESS_KEY_ID is required")
            
        if not self.AWS_SECRET_ACCESS_KEY:
            errors.append("AWS_SECRET_ACCESS_KEY is required")
        
        if errors:
            raise ValueError(f"Configuration errors: {', '.join(errors)}")
        
        return True

# Global config instance
config = Config()
if __name__ == "__main__":
    try:
        config.validate()
        print("Configuration is valid.")
        print(f"MongoDB URI: {config.mongodb_uri}")
        print(f"S3 Warehouse Path: {config.s3_warehouse_path}")
    except Exception as e:
        print(f"Configuration error: {e}")
