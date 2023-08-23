import os
import boto3
import gzip
import pyspark.sql.functions as F
from io import BytesIO
from pyspark.sql import SparkSession
from sqlalchemy import create_engine
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'Maher',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'email_on_failure': True,
    # 'email_on_retry': True,
}

dag = DAG(
    dag_id="etl_pipeline",
    default_args={"start_date": datetime.datetime.today()},
    schedule_interval=timedelta(minutes=30),
)


def extract_data(**kwargs):
    """Extracts the data from the source.

    Returns:
      The extracted data.
    """

    try:
        logging.info("Extracting data from the source.")
        # ... (rest of the code)
    except Exception as e:
        logging.error("Error occurred during data extraction: %s", str(e))
        raise


    # Load environment variables
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION')
    aws_bucket_name = os.getenv('AWS_BUCKET_NAME')
    connection_string = os.getenv('POSTGRES_CONNECTION_STRING')

    # Creating Boto3 Session
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    s3 = session.client('s3')

    # Creating Spark Session
    spark = SparkSession.builder \
        .master("local") \
        .appName("ETL-Pipeline") \
        .config("spark.jars", ROOT_DIR+"/postgresql-42.6.0.jar") \
        .config("spark.driver.extraClassPath", ROOT_DIR+"/postgresql-42.6.0.jar") \
        .config("spark.executor.extraClassPath", ROOT_DIR+"/postgresql-42.6.0.jar") \
        .getOrCreate()

    # Download files from S3 and load into Spark DataFrame
    # ... (code for downloading and processing S3 data)
    df.show(truncate=False)

    # Extract data from PostgreSQL into Spark DataFrame
    six_months_ago = datetime.now() - timedelta(days=180)
    six_months_ago = six_months_ago.strftime("%Y-%m-%d")
    where_clause = "(SELECT * FROM tk_2023_07 WHERE date >= '" + six_months_ago + "') as tk"
    postgres_data = spark.read.format("jdbc") \
        .option("url", os.getenv('POSTGRES_CONNECTION_JDBC_STRING')) \
        .option("dbtable", where_clause) \
        .option("user", os.getenv('POSTGRES_USER')) \
        .option("password", os.getenv('POSTGRES_PASSWORD')) \
        .option("driver", "org.postgresql.Driver") \
        .load()

    # Find difference between the two dataframes when postgres_data count is greater than 0
    if postgres_data_count > 0:
        changes_df = df.join(postgres_data, df.posting_id == postgres_data.posting_id, how='left_anti')
    else:
        changes_df = df

    # Push the changes_df to XCom
    kwargs['ti'].xcom_push(key='extracted_data', value=changes_df)
    # return changes_df



def transform_data(**kwargs):
    """Transforms the extracted data.

    Args:
        extracted_data: The data extracted from the source.

    Returns:
        The transformed data.
    """

    try:
        # Your existing transform_data code
        logging.info("Transforming data.")
        # ... (rest of the code)
    except Exception as e:
        logging.error("Error occurred during data transformation: %s", str(e))
        raise

        # Get the extracted data from the XCom of the previous task
    extracted_data = kwargs['ti'].xcom_pull(task_ids='extract_data', key='extracted_data')


    # Create a new column for the duration of the job posting in days
    # Change the data types of the columns.
    df_types_fixed = extracted_data.withColumn("job_id", extracted_data.job_id.cast(T.StringType())) \
        .withColumn("posting_count", extracted_data.posting_count.cast(T.LongType())) \
        .withColumn("source_website_count", extracted_data.source_website_count.cast(T.LongType())) \
        .withColumn("date", extracted_data.date.cast(T.DateType())) \
        .withColumn("expiration_date", extracted_data.expiration_date.cast(T.DateType())) \
        .withColumn("duration", extracted_data.duration.cast(T.LongType())) \
        .withColumn("salary", extracted_data.salary.cast(T.LongType())) \
        .withColumn("salary_from", extracted_data.salary_from.cast(T.LongType())) \
        .withColumn("salary_to", extracted_data.salary_to.cast(T.LongType())) \
        .withColumn("experience_years_from", extracted_data.experience_years_from.cast(T.LongType())) \
        .withColumn("experience_years_to", extracted_data.experience_years_to.cast(T.LongType())) \
        .withColumn("hours_per_week_from", extracted_data.hours_per_week_from.cast(T.LongType())) \
        .withColumn("hours_per_week_to", extracted_data.hours_per_week_to.cast(T.LongType())) \
        .withColumn('working_hours_type', extracted_data.working_hours_type.cast(T.IntegerType())) \
        .withColumn('advertiser_type', extracted_data.advertiser_type.cast(T.StringType())) \
        .withColumn('contract_type', extracted_data.contract_type.cast(T.StringType())) \
        .withColumn('education_level', extracted_data.education_level.cast(T.StringType())) \
        .withColumn('employment_type', extracted_data.employment_type.cast(T.StringType())) \
        .withColumn('experience_level', extracted_data.experience_level.cast(T.StringType())) \
        .withColumn("it_skills", F.concat_ws(",", extracted_data.it_skills.value)) \
        .withColumn("language_skills", F.concat_ws(",", extracted_data.language_skills.value)) \
        .withColumn('organization_activity', extracted_data.organization_activity.cast(T.StringType())) \
        .withColumn('organization_industry', extracted_data.organization_industry.cast(T.StringType())) \
        .withColumn('organization_region', extracted_data.organization_region.cast(T.StringType())) \
        .withColumn('organization_size', extracted_data.organization_size.cast(T.StringType())) \
        .withColumn('profession', extracted_data.profession.cast(T.StringType())) \
        .withColumn('profession_class', extracted_data.profession_class.cast(T.StringType())) \
        .withColumn('profession_group', extracted_data.profession_group.cast(T.StringType())) \
        .withColumn('profession_isco_code', extracted_data.profession_isco_code.cast(T.StringType())) \
        .withColumn('profession_kldb_code', extracted_data.profession_kldb_code.cast(T.StringType())) \
        .withColumn('profession_onet_2019_code', extracted_data.profession_onet_2019_code.cast(T.StringType())) \
        .withColumn("professional_skills", F.concat_ws(",", extracted_data.professional_skills.value)) \
        .withColumn('region', extracted_data.region.cast(T.StringType())) \
        .withColumn('organization_national_id', F.substring(extracted_data.organization_national_id, 1, 25)) \
        .withColumn("soft_skills", F.concat_ws(",", extracted_data.soft_skills.value)) \
        .withColumn('source_type', extracted_data.source_type.cast(T.StringType())) \
        .withColumn('advertiser_email', F.concat_ws(",", F.slice(F.split(extracted_data.advertiser_email, ','), 1, 5))) \
        .withColumn('advertiser_website', F.concat_ws(",", F.slice(F.split(extracted_data.advertiser_website, ','), 1, 5))) \
        .withColumn('advertiser_contact_person', F.substring(extracted_data.advertiser_contact_person, 1, 255)) \
        .withColumn('advertiser_reference_number', F.substring(extracted_data.advertiser_reference_number, 1, 255)) \
        .withColumn('organization_website', F.substring(extracted_data.organization_website, 1, 100)) \
        .withColumn('organization_linkedin_id', F.substring(extracted_data.organization_linkedin_id, 1, 255)) \
        .withColumn('apply_url', F.substring(extracted_data.apply_url, 1, 255)) \
        .withColumn('source_url', F.substring(extracted_data.source_url, 1, 255)) \
        .withColumn('source_website', F.substring(extracted_data.source_website, 1, 255)) \
        .withColumn('advertiser_name', F.substring(extracted_data.advertiser_name, 1, 255)) \
        .withColumn('advertiser_phone', F.concat_ws(",", F.slice(F.split(extracted_data.advertiser_phone, ','), 1, 10)))

    df_types_fixed = df_types_fixed.withColumn("it_skills", F.when(df_types_fixed.it_skills == "", None).otherwise(df_types_fixed.it_skills)) \
    .withColumn("language_skills", F.when(df_types_fixed.language_skills == "", None).otherwise(df_types_fixed.language_skills)) \
    .withColumn("professional_skills", F.when(df_types_fixed.professional_skills == "", None).otherwise(df_types_fixed.professional_skills)) \
    .withColumn("soft_skills", F.when(df_types_fixed.soft_skills == "", None).otherwise(df_types_fixed.soft_skills)) \


    # Drop duplicates in posting_id column and keep the latest one
    df_deduplicated = df_types_fixed.dropDuplicates(['posting_id'])

    # Push the df_deduplicated to XCom
    kwargs['ti'].xcom_push(key='transformed_data', value=df_deduplicated)
    return df_deduplicated


def load_data(**kwargs):
    """Loads the transformed data into the Postgres warehouse.

    Args:
        transformed_data: The data after transformation.

    Returns:
        None.
    """
    try:
        # Your existing load_data code
        logging.info("Loading data into the warehouse.")
        # ... (rest of the code)
    except Exception as e:
        logging.error("Error occurred during data loading: %s", str(e))
        raise

    # Get the transformed data from the XCom of the previous task
    transformed_data = kwargs['ti'].xcom_pull(task_ids='transform_data', key='transformed_data')

    # Write the data to a table in Postgres
    transformed_data.write.format("jdbc") \
        .option("url", os.getenv('POSTGRES_CONNECTION_JDBC_STRING')) \
        .option("dbtable", os.getenv('POSTGRES_TABLE')) \
        .option("user", os.getenv('POSTGRES_USER')) \
        .option("password", os.getenv('POSTGRES_PASSWORD')) \
        .option("driver", "org.postgresql.Driver") \
        .mode("append") \
        .save()

    print("Data loaded to Postgres warehouse.")


extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_data,
    dag=dag,
    depends_on_past=True,
)

load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag,
    depends_on_past=True,
)

extract_task >> transform_task >> load_task
