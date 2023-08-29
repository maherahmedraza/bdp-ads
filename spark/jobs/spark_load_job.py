import sys
from pyspark.sql import SparkSession


# Get arguments
postgres_jdbc_url = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]
postgres_table = sys.argv[4]

# Create spark session
spark = (SparkSession.builder.appName("Spark-Load")
         .config("spark.local.dir", "/tmp/spark-temp")  # Set local dir
         .config("spark.executor.memory", "4g")  # Set executor memory
         .config("spark.driver.memory", "2g")  # Set driver memory
         .config("spark.executor.cores", 4)  # Set number of executor cores
         .config("spark.default.parallelism", 100)  # Set default parallelism
         .config("spark.sql.shuffle.partitions", 100)  # Set shuffle partitions
         .config("spark.memory.offHeap.enabled", True)  # Enable off-heap memory
         .config("spark.memory.offHeap.size", "1g")  # Set off-heap memory size
         .config("spark.sql.autoBroadcastJoinThreshold", -1)  # Disable auto-broadcasting
         .config("spark.sql.parquet.enableVectorizedReader", "false")
         .getOrCreate())

# Read parquet file
df = spark.read.parquet("/opt/airflow/data/final/finalized_ads.parquet")
# df = spark.read.parquet("/opt/airflow/data/processed/transformed_ads.parquet")
df.printSchema()

df.write.format("jdbc") \
    .option("url", postgres_jdbc_url) \
    .option("dbtable", postgres_table) \
    .option("user", postgres_user) \
    .option("password", postgres_pwd) \
    .option("driver", "org.postgresql.Driver") \
    .mode("append") \
    .save()

# Stop spark session
spark.stop()