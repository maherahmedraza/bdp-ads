import sys
from pyspark.sql import SparkSession


# Get arguments
postgres_jdbc_url = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]
postgres_table = sys.argv[4]

# Create spark session
spark = (SparkSession.builder.appName("Spark-Load")
         .master("spark://spark:7077")   # Set Spark to run in local mode using all available cores
         .config("spark.local.dir", "/tmp/spark-temp")  # Set local dir
         .config("spark.executor.instances", 2)
         .config("spark.executor.cores", "2")  # Set executor cores to 2
         .config("spark.executor.memory", "4g")  # Increase executor memory to 6g
         .config("spark.driver.memory", "4g")  # Set driver memory to 4g
         .config("spark.sql.shuffle.partitions", "64")  # Adjust the number of shuffle partitions to 4
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  # Use KryoSerializer
         .config("spark.memory.offHeap.enabled", "false")  # Disable off-heap memory
         .config("spark.sql.autoBroadcastJoinThreshold", "-1")  # Disable auto-broadcasting
         .config("spark.sql.parquet.enableVectorizedReader", "false")  # Disable vectorized Parquet reader
         .getOrCreate())

# Read parquet file
df = spark.read.parquet("/opt/airflow/data/final/finalized_ads.parquet")
# df = spark.read.parquet("/opt/airflow/data/processed/transformed_ads.parquet")
df.printSchema()
# Check if the DataFrame is empty
if df.isEmpty():
    # Handle the case where the DataFrame is empty
    print("DataFrame is empty. No further processing needed.")
else:
    # write data to postgres database
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
