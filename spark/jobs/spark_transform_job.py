import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Create spark session
spark = (SparkSession.builder.appName("Spark-Transform")
         .config("spark.local.dir", "/tmp/spark-temp")  # Set local dir
         .config("spark.executor.memory", "4g")  # Set executor memory
         .config("spark.driver.memory", "2g")  # Set driver memory
         .config("spark.executor.cores", 4)  # Set number of executor cores
         .config("spark.default.parallelism", 100)  # Set default parallelism
         .config("spark.sql.shuffle.partitions", 100)  # Set shuffle partitions
         .config("spark.memory.offHeap.enabled", True)  # Enable off-heap memory
         .config("spark.memory.offHeap.size", "2g")  # Set off-heap memory size
         .config("spark.sql.autoBroadcastJoinThreshold", -1)  # Disable auto-broadcasting
         .config("spark.sql.parquet.enableVectorizedReader", "false")
         .getOrCreate())

# Read parquet file
df = spark.read.parquet("/opt/airflow/data/processed/all_changes_ads.parquet")
df.printSchema()

# Drop duplicates
df = df.dropDuplicates()

# Replace empty string with None for all string columns
# df = df.withColumn("job_id", F.when(df.job_id == "", None).otherwise(df.job_id)) \
#     .withColumn("job_title", F.when(df.job_title == "", None).otherwise(df.job_title)) \
#     .withColumn("job_description", F.when(df.job_description == "", None).otherwise(df.job_description)) \
#     .withColumn("job_requirements", F.when(df.job_requirements == "", None).otherwise(df.job_requirements)) \
#     .withColumn("job_benefits", F.when(df.job_benefits == "", None).otherwise(df.job_benefits)) \
#     .withColumn("job_category", F.when(df.job_category == "", None).otherwise(df.job_category)) \
#     .withColumn("job_sub_category", F.when(df.job_sub_category == "", None).otherwise(df.job_sub_category)) \
#     .withColumn("job_type", F.when(df.job_type == "", None).otherwise(df.job_type)) \
#     .withColumn("job_location", F.when(df.job_location == "", None).otherwise(df.job_location)) \
#     .withColumn("job_posting_date", F.when(df.job_posting_date == "", None).otherwise(df.job_posting_date)) \
#     .withColumn("job_expiration_date", F.when(df.job_expiration_date == "", None).otherwise(df.job_expiration_date)) \
#     .withColumn("job_duration", F.when(df.job_duration == "", None).otherwise(df.job_duration)) \
#     .withColumn("job_salary", F.when(df.job_salary == "", None).otherwise(df.job_salary)) \
#     .withColumn("job_salary_from", F.when(df.job_salary_from == "", None).otherwise(df.job_salary_from)) \
#     .withColumn("job_salary_to", F.when(df.job_salary_to == "", None).otherwise(df.job_salary_to)) \
#     .withColumn(
#     "job_experience_years_from", F.when(df.job_experience_years_from == "", None).otherwise(
#         df.job_experience_years_from)) \
#     .withColumn(
#     "job_experience_years_to", F.when(df.job_experience_years_to == "", None).otherwise(
#         df.job_experience_years_to)) \
#     .withColumn(
#     "job_hours_per_week_from", F.when(df.job_hours_per_week_from == "", None).otherwise(
#         df.job_hours_per_week_from)) \
#     .withColumn(
#     "job_hours_per_week_to", F.when(df.job_hours_per_week_to == "", None).otherwise(
#         df.job_hours_per_week_to)) \
#     .withColumn(
#     "job_working_hours_type", F.when(df.job_working_hours_type == "", None).otherwise(
#         df.job_working_hours_type)) \
#     .withColumn("job_advertiser_type", F.when(df.job_advertiser_type == "", None).otherwise(df.job_advertiser_type)) \
#     .withColumn("job_contract_type", F.when(df.job_contract_type == "", None).otherwise(df.job_contract_type)) \
#     .withColumn("job_education_level", F.when(df.job_education_level == "", None).otherwise(df.job_education_level)) \
#     .withColumn("job_employment_type", F.when(df.job_employment_type == "", None).otherwise(df.job_employment_type)) \
#     .withColumn(
#     "job_experience_level", F.when(df.job_experience_level == "", None).otherwise(
#         df.job_experience_level)) \
#     .withColumn("job_it_skills", F.when(df.job_it_skills == "", None).otherwise(df.job_it_skills)) \
#     .withColumn("job_language_skills", F.when(df.job_language_skills == "", None).otherwise(df.job_language_skills)) \
#     .withColumn(
#     "job_professional_skills", F.when(df.job_professional_skills == "", None).otherwise(
#         df.job_professional_skills)) \
#     .withColumn("job_soft_skills", F.when(df.job_soft_skills == "", None).otherwise(df.job_soft_skills)) \
#     .withColumn(
#     "job_organization_activity", F.when(df.job_organization_activity == "", None).otherwise(
#         df.job_organization_activity)) \
#     .withColumn(
#     "job_organization_industry", F.when(df.job_organization_industry == "", None).otherwise(
#         df.job_organization_industry)) \
#     .withColumn(
#     "job_organization_region", F.when(df.job_organization_region == "", None).otherwise(
#         df.job_organization_region)) \
#     .withColumn(
#     "job_organization_size", F.when(df.job_organization_size == "", None).otherwise(
#         df.job_organization_size)) \
#     .withColumn("job_profession", F.when(df.job_profession == "", None).otherwise(df.job_profession)) \


# Replace empty string with None
df = df.withColumn("it_skills", F.when(df.it_skills == "", None).otherwise(df.it_skills)) \
    .withColumn("language_skills", F.when(df.language_skills == "", None).otherwise(df.language_skills)) \
    .withColumn("professional_skills", F.when(df.professional_skills == "", None).otherwise(df.professional_skills)) \
    .withColumn("soft_skills", F.when(df.soft_skills == "", None).otherwise(df.soft_skills)) \

# Write changes to Parquet file "/opt/airflow/data/processed/transformed_ads.parquet"
df.write.parquet("/opt/airflow/data/final/finalized_ads.parquet", mode="overwrite")
print("Changes written to Parquet file")

# Stop Spark session
spark.stop()

#%%
