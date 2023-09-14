import sys
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

# Get arguments
# postgres_jdbc_url, postgres_user, postgres_pwd, filesToLoadInDF = sys.argv[1:5]
postgres_jdbc_url = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]
filesToLoadInDF = sys.argv[4].split(',')
print(f"filesToLoadInDF {sys.argv[4]} \n {len(filesToLoadInDF)}", )

# Print files to load
for file in filesToLoadInDF:
    print(file)

# Create spark session
spark = (SparkSession.builder.appName("Spark-Extract")
         # .master("spark://spark:7077")  # Run Spark locally with all cores?
         # # .config("spark.driver.host", "spark")  # Set driver host
         # .config("spark.driver.cores", 2)  # Set driver cores
         # .config("spark.executor.instances", 2)  # Set number of executors
         .config("spark.local.dir", "/tmp/spark-temp")  # Set local dir
         .config("spark.executor.memory", "4g")  # Set executor memory
         .config("spark.driver.memory", "4g")  # Set driver memory
         .config("spark.executor.cores", 4)  # Set number of executor cores
         .config("spark.default.parallelism", 100)  # Set default parallelism
         .config("spark.sql.shuffle.partitions", 100)  # Set shuffle partitions
         .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")  # Use KryoSerializer
         .config("spark.memory.offHeap.enabled", "false")  # Enable off-heap memory
         # .config("spark.memory.offHeap.size", "2g")  # Set off-heap memory size
         .config("spark.sql.autoBroadcastJoinThreshold", -1)  # Disable auto-broadcasting
         .getOrCreate())

df = spark.read.json(filesToLoadInDF)
df.printSchema()
print("Rows loaded from S3 ", df.count())

# Load data from Postgres
today = datetime.now()
end_date = today.replace(day=1) - timedelta(days=1)
start_date = end_date - timedelta(days=180)  # 180 days for six months

# Adjust the start_date to the first day of the month
start_date = start_date.replace(day=1)

start_date_str = start_date.strftime("%Y-%m-%d")
print(start_date_str)
end_date_str = end_date.strftime("%Y-%m-%d")
print(end_date_str)

where_clause = f"(SELECT * FROM tk_2023_07 WHERE date >= '{start_date_str}' AND date <= '{end_date_str}') as tk"

# Load data from Postgres
postgres_data = (
    spark.read
    .format("jdbc")
    .option("url", postgres_jdbc_url)
    .option("dbtable", where_clause)
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .option("spark.driver.extraClassPath", "/opt/airflow/spark/assets/jars/postgresql-42.6.0.jar")
    .option("spark.executor.extraClassPath", "/opt/airflow/spark/assets/jars/postgresql-42.6.0.jar")
    .option("driver", "org.postgresql.Driver")
    .load()
)

postgres_data.printSchema()
postgres_data_count = postgres_data.count()
print("Rows loaded from Postgres ", postgres_data_count)

# Find difference between the two dataframes when postgres_data count is greater than 0
if postgres_data_count > 0:
    changes_df = df.join(postgres_data, ["posting_id"], "left_anti")
    # changes_df = df.subtract(postgres_data)
else:
    changes_df = df

# Taking a count of changes_df
changes_df_count = changes_df.count()
print("Rows to be inserted ", changes_df_count)

# Write changes to Parquet file "/opt/airflow/data/processed/all_changes_ads.parquet"
if changes_df_count > 0:
    changes_df.write.parquet("/opt/airflow/data/processed/all_changes_ads.parquet", mode="overwrite")
    print("Changes written to Parquet file")

# Stop the Spark session
spark.stop()
