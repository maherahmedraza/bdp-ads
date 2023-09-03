import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import pyspark.sql.types as T
from datetime import datetime, timedelta

# Get arguments
# postgres_jdbc_url, postgres_user, postgres_pwd, filesToLoadInDF = sys.argv[1:5]
postgres_jdbc_url = sys.argv[1]
postgres_user = sys.argv[2]
postgres_pwd = sys.argv[3]
filesToLoadInDF = sys.argv[4].split(',')
print(f"filesToLoadInDF {sys.argv[4]} \n {len(filesToLoadInDF)}", )

for file in filesToLoadInDF:
    print(file)

# Create spark session
# spark = (SparkSession.builder.appName("Spark Extract")
#          .config("spark.local.dir", "/tmp/spark-temp") \
#          .getOrCreate())

spark = (SparkSession.builder.appName("Spark-Extract")
         # .master("spark://spark:7077")  # Run Spark locally with all cores?
         # # .config("spark.driver.host", "spark")  # Set driver host
         # .config("spark.driver.cores", 2)  # Set driver cores
         # .config("spark.executor.instances", 1)  # Set number of executors
         .config("spark.local.dir", "/tmp/spark-temp")  # Set local dir
         .config("spark.executor.memory", "4g")  # Set executor memory
         .config("spark.driver.memory", "2g")  # Set driver memory
         .config("spark.executor.cores", 2)  # Set number of executor cores
         .config("spark.default.parallelism", 64)  # Set default parallelism
         .config("spark.sql.shuffle.partitions", 64)  # Set shuffle partitions
         .config("spark.memory.offHeap.enabled", True)  # Enable off-heap memory
         .config("spark.memory.offHeap.size", "1g")  # Set off-heap memory size
         .config("spark.sql.autoBroadcastJoinThreshold", -1)  # Disable auto-broadcasting
         .getOrCreate())

df = spark.read.json(filesToLoadInDF)
df.printSchema()
print("Rows loaded from S3 ", df.count())

df = df.withColumn("job_id", df.job_id.cast(T.StringType())) \
    .withColumn("posting_count", df.posting_count.cast(T.LongType())) \
    .withColumn("source_website_count", df.source_website_count.cast(T.LongType())) \
    .withColumn("date", df.date.cast(T.DateType())) \
    .withColumn("expiration_date", df.expiration_date.cast(T.DateType())) \
    .withColumn("duration", df.duration.cast(T.LongType())) \
    .withColumn("salary", df.salary.cast(T.LongType())) \
    .withColumn("salary_from", df.salary_from.cast(T.LongType())) \
    .withColumn("salary_to", df.salary_to.cast(T.LongType())) \
    .withColumn("experience_years_from", df.experience_years_from.cast(T.LongType())) \
    .withColumn("experience_years_to", df.experience_years_to.cast(T.LongType())) \
    .withColumn("hours_per_week_from", df.hours_per_week_from.cast(T.LongType())) \
    .withColumn("hours_per_week_to", df.hours_per_week_to.cast(T.LongType())) \
    .withColumn('working_hours_type', df.working_hours_type.value.cast(T.IntegerType())) \
    .withColumn('advertiser_type', df.advertiser_type.value.cast(T.StringType())) \
    .withColumn('contract_type', df.contract_type.value.cast(T.StringType())) \
    .withColumn('education_level', df.education_level.value.cast(T.StringType())) \
    .withColumn('employment_type', df.employment_type.value.cast(T.StringType())) \
    .withColumn('experience_level', df.experience_level.value.cast(T.StringType())) \
    .withColumn("it_skills", F.concat_ws(",", df.it_skills.value)) \
    .withColumn("language_skills", F.concat_ws(",", df.language_skills.value)) \
    .withColumn('organization_activity', df.organization_activity.value.cast(T.StringType())) \
    .withColumn('organization_industry', df.organization_industry.value.cast(T.StringType())) \
    .withColumn('organization_region', df.organization_region.value.cast(T.StringType())) \
    .withColumn('organization_size', df.organization_size.value.cast(T.StringType())) \
    .withColumn('profession', df.profession.value.cast(T.StringType())) \
    .withColumn('profession_class', df.profession_class.value.cast(T.StringType())) \
    .withColumn('profession_group', df.profession_group.value.cast(T.StringType())) \
    .withColumn('profession_isco_code', df.profession_isco_code.value.cast(T.StringType())) \
    .withColumn('profession_kldb_code', df.profession_kldb_code.value.cast(T.StringType())) \
    .withColumn('profession_onet_2019_code', df.profession_onet_2019_code.value.cast(T.StringType())) \
    .withColumn("professional_skills", F.concat_ws(",", df.professional_skills.value)) \
    .withColumn('region', df.region.value.cast(T.StringType())) \
    .withColumn('organization_national_id', F.substring(df.organization_national_id, 1, 25)) \
    .withColumn("soft_skills", F.concat_ws(",", df.soft_skills.value)) \
    .withColumn('source_type', df.source_type.value.cast(T.StringType())) \
    .withColumn('advertiser_email', F.concat_ws(",", F.slice(F.split(df.advertiser_email, ','), 1, 5))) \
    .withColumn('advertiser_website', F.concat_ws(",", F.slice(F.split(df.advertiser_website, ','), 1, 5))) \
    .withColumn('advertiser_contact_person', F.substring(df.advertiser_contact_person, 1, 255)) \
    .withColumn('advertiser_reference_number', F.substring(df.advertiser_reference_number, 1, 255)) \
    .withColumn('organization_website', F.substring(df.organization_website, 1, 100)) \
    .withColumn('organization_linkedin_id', F.substring(df.organization_linkedin_id, 1, 255)) \
    .withColumn('apply_url', F.substring(df.apply_url, 1, 255)) \
    .withColumn('source_url', F.substring(df.source_url, 1, 255)) \
    .withColumn('source_website', F.substring(df.source_website, 1, 255)) \
    .withColumn('advertiser_name', F.substring(df.advertiser_name, 1, 255)) \
    .withColumn('advertiser_phone', F.concat_ws(",", F.slice(F.split(df.advertiser_phone, ','), 1, 10))) \


df.printSchema()

# Load data from Postgres
six_months_ago = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
where_clause = f"(SELECT * FROM tk_2023_07 WHERE date >= '{six_months_ago}') as tk"

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
    # changes_df = df.join(postgres_data, df.columns, "left_anti")
    changes_df = df.join(postgres_data, ["posting_id"], "left_anti")
    # changes_df = df.subtract(postgres_data)
else:
    changes_df = df

# changes_df.show(truncate=False)
changes_df_count = changes_df.count()
print("Rows to be inserted ", changes_df_count)

# //////////// TRANSFORM //////////////

# # # Drop duplicates
# df = df.dropDuplicates()
#
# # Replace empty string with None
# df = df.withColumn("it_skills", F.when(df.it_skills == "", None).otherwise(df.it_skills)) \
#     .withColumn("language_skills", F.when(df.language_skills == "", None).otherwise(df.language_skills)) \
#     .withColumn("professional_skills", F.when(df.professional_skills == "", None).otherwise(df.professional_skills)) \
#     .withColumn("soft_skills", F.when(df.soft_skills == "", None).otherwise(df.soft_skills)) \


# Write changes to Parquet file "/opt/airflow/data/raw/all_changes_ads.parquet"
if changes_df_count > 0:
    changes_df.write.parquet("/opt/airflow/data/processed/all_changes_ads.parquet", mode="overwrite")
    print("Changes written to Parquet file")

# Stop the Spark session
spark.stop()
