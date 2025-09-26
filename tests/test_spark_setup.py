# test_hadoop_windows.py
import os
import tempfile
from pyspark.sql import SparkSession

def test_hadoop_windows_setup():
    """Test if Windows Hadoop utilities are working"""
    
    print("=== Testing Windows Hadoop Setup ===")
    
    # Check if files exist
    spark_home = os.getenv('SPARK_HOME')
    if not spark_home:
        print("SPARK_HOME not set")
        return
    
    hadoop_home = os.getenv('HADOOP_HOME')
    if not hadoop_home:
        print(" HADOOP_HOME not set")
        return
    
    winutils_path = os.path.join(hadoop_home, 'bin', 'winutils.exe')
    hadoop_dll_path = os.path.join(spark_home, 'bin', 'hadoop.dll')
    
    print(f"SPARK_HOME: {spark_home}")
    print(f"winutils.exe: {'✓' if os.path.exists(winutils_path) else '**'} {winutils_path}")
    print(f"hadoop.dll: {'✓' if os.path.exists(hadoop_dll_path) else '**'} {hadoop_dll_path}")
    
    if not os.path.exists(winutils_path):
        print(" winutils.exe not found! Download it first.")
        return
    
    # Test Spark with file operations
    try:
         # Create temp directory with proper permissions
        temp_dir = tempfile.mkdtemp()
        print(f"Using temp directory: {temp_dir}")
        
        spark = SparkSession.builder \
            .appName("Hadoop-Windows-Test") \
            .master("local[2]") \
            .config("spark.sql.warehouse.dir", f"file:///{temp_dir}/spark-warehouse") \
            .config("spark.hadoop.fs.defaultFS", "file:///") \
            .getOrCreate()
        
        print("✓ Spark session created successfully")
        
        # Test basic operations
        data = [(1, "test1"), (2, "test2"), (3, "test3")]
        df = spark.createDataFrame(data, ["id", "value"])
        
        # Test simple operations (no file I/O initially)
        result = df.count()
        print(f"DataFrame operations work: {result} records")

        
        # Test writing with proper path format
        output_path = os.path.join(temp_dir, "test_output").replace("\\", "/")
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(output_path)
        
        print("✓ File write operation successful")
    
        
        spark.stop()
        print("All tests passed!")
        
    except Exception as e:
        print(f"Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_hadoop_windows_setup()