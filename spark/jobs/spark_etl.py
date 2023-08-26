import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Create spark session
spark = SparkSession.builder.appName("Spark ETL").getOrCreate()


####################################
# Parameters
####################################
postgres_jdbc_url = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]

####################################
# Read Postgres
####################################
print("######################################")
print("READING POSTGRES TABLES")
print("######################################")

six_months_ago = datetime.now() - timedelta(days=70)
six_months_ago = six_months_ago.strftime("%Y-%m-%d")
# where_clause = "(SELECT * FROM tk_2023_07 WHERE date >= '" + six_months_ago + "') as tk"
where_clause = "(SELECT * FROM tk_2023_07 WHERE date >= '" + six_months_ago + "') as tk"

df_ads = (
    spark.read
    .format("jdbc")
    .option("url", postgres_jdbc_url)
    .option("dbtable", where_clause)
    .option("user", postgres_user)
    .option("password", postgres_pwd)
    .load()
)

print(df_ads.count())