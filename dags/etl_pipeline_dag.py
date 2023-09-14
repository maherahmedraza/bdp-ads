from __future__ import annotations

# [START import_module]
import json
from textwrap import dedent

import pendulum
import boto3
import gzip
import os
import logging
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, Boolean, Text, BigInteger
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import ProgrammingError
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

# [END import_module]


# spark_master = "spark://spark:7077"
config_file = "/opt/airflow/spark/assets/configs/config.json"
with open(config_file, 'r') as f:
    config = json.load(f)

# declare str global var for list of files to load in spark
s3_files_list = ""


default_args = {
    'owner': 'Maher',
    'depends_on_past': False,
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'email': ['maherahmedraza@uni-koblenz.de'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        "etl_pipeline_dag",
        default_args={"retries": 2},
        # [END default_args]
        description="A simple ETL pipeline using Airflow and Spark.",
        #start today now
        start_date=datetime(2023, 8, 27),
        schedule_interval=timedelta(hours=3),
        catchup=False,
        tags=["etl","ads"],
) as dag:


    # The function to extract data from S3
    def extract_s3(**kwargs):
        # Load environment variables
        aws_access_key_id = config['AWS_ACCESS_KEY_ID']
        aws_secret_access_key = config['AWS_SECRET_ACCESS_KEY']
        aws_region = config['AWS_REGION']
        aws_bucket_name = config['AWS_BUCKET_NAME']

        # Creating Boto3 Session
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )

        # logging.info("S3 session created", session)
        s3 = session.client('s3')

        # Get the list of objects in the S3 bucket
        prefix = 'DE/monthly/'
        response = s3.list_objects_v2(Bucket=aws_bucket_name, Prefix=prefix, Delimiter='/')
        logging.info("S3 response", response)

        #Number of Months to download
        months = 6
        # Number of files per month to download
        files_per_month = 1
        # current project directory parent path
        ROOT_DIR = os.path.abspath(os.pardir)

        # Get the list of subfolders in the S3 bucket
        subfolders = [obj['Prefix'] for obj in response['CommonPrefixes']]
        # Get the last N subfolders - N = months of data to download
        subfolders = subfolders[-months:]


        filesToLoadInDF = []
        # Download files from each subfolder
        for subfolder in subfolders:
            # Get the list of files in the subfolder
            response = s3.list_objects_v2(Bucket=aws_bucket_name, Prefix=subfolder)
            # Get the file paths
            files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.jsonl.gz')]
            # Only get the first N files
            files = files[:files_per_month]

            # filesToLoadInDF = [filesToLoadInDF.append(f) for f in files]

            # Create the folder in your local machine
            folder = "/opt/airflow/data/raw/" + aws_bucket_name + "/" + subfolder
            if not os.path.exists(folder):
                oldmask = os.umask(000)
                os.makedirs(folder, 0o777)
                # os.umask(odmask)


            # Download and extract each file
            for file in files:
                filename = file.rsplit("/", 1)[1]
                print('Downloading file {}...'.format(filename))
                print(subfolder + filename)
                print(folder + filename)

                # Check if the file already exists
                localExtractedFilePath = os.path.join(folder + filename[:-3])
                if not os.path.exists(localExtractedFilePath):
                    # Download and Save the file
                    s3.download_file(Filename=folder + filename, Bucket=aws_bucket_name, Key=subfolder + filename)

                    locaFilePath = os.path.join(folder + filename)
                    print(localExtractedFilePath)
                    filesToLoadInDF.append(localExtractedFilePath)
                    # Extract the data from the gzipped file
                    with gzip.open(locaFilePath, 'rb') as gz_file, open(localExtractedFilePath, 'wb') as extract_file:
                        extract_file.write(gz_file.read())

                    # Delete the gzipped file
                    os.remove(locaFilePath)
                else:
                    filesToLoadInDF.append(localExtractedFilePath)
                    print('File already exists. Skipping...')

        print(filesToLoadInDF)

        s3_files_list = ','.join(filesToLoadInDF)
        print(s3_files_list)

        return s3_files_list
        # Pushing list of files path to xcom
        # ti = kwargs["ti"]
        # ti.xcom_push("filesToLoadInDF", filesToLoadInDF)


    # check if table exists
    # if not exists than create
    def check_db(**kwargs):

        # Create a connection to Postgres
        # connection_string = "postgresql+psycopg2://airflow:airflow@postgres/job_ads_db"
        connection_string = config['POSTGRES_CONNECTION_STRING']
        # TODO : Write code check DB and create DB if not esists

        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        print(engine)
        pgconn = engine.connect()
        print(pgconn)

        metadata = MetaData(bind=pgconn)

        table_name = config["POSTGRES_TABLE"]
        print(table_name)
        if pgconn.dialect.has_table(pgconn, table_name):
            print('Table already exists.')
        else:
            print('Creating table...')
            table = Table(table_name, metadata,
                          Column('id', Integer, primary_key=True),
                          Column('job_id', String(32)),
                          Column('posting_count', Integer),
                          Column('source_website_count', Integer),
                          Column('date', Date),
                          Column('sequence_number', BigInteger),
                          Column('expiration_date', Date),
                          Column('expired', Boolean),
                          Column('duration', Integer),
                          Column('source_url', String(255)),
                          Column('source_website', String(255)),
                          Column('source_type', String(2)),
                          # Column('duplicate', Boolean),
                          # Column('first_posting', Boolean),
                          Column('posting_id', String(32)),
                          Column('duplicate_on_jobsite', Boolean),
                          Column('via_intermediary', Boolean),
                          Column('language', String(3)),
                          Column('job_title', String(255)),
                          Column('profession', String(4)),
                          Column('profession_group', String(4)),
                          Column('profession_class', String(4)),
                          Column('profession_isco_code', String(10)),
                          Column('location', String(5)),
                          Column('location_name', String(255)),
                          Column('location_coordinates', String(30)),
                          Column('location_remote_possible', Boolean),
                          Column('region', String(2)),
                          Column('education_level', String(2)),
                          Column('advertiser_name', String(255)),
                          Column('advertiser_type', String(2)),
                          Column('advertiser_street', String(255)),
                          Column('advertiser_postal_code', String(15)),
                          Column('advertiser_location', String(255)),
                          Column('advertiser_phone', String(255)),
                          Column('available_contact_fields', String(100)),
                          # Column('organization', Integer),
                          Column('organization_name', String(255)),
                          Column('organization_industry', String(2)),
                          Column('organization_activity', String(10)),
                          Column('organization_size', String(2)),
                          Column('organization_address', String(255)),
                          Column('organization_street_number', String(100)),
                          Column('organization_postal_code', String(5)),
                          Column('organization_location', String(5)),
                          Column('organization_location_name', String(255)),
                          Column('organization_region', String(2)),
                          Column('contract_type', String(2)),
                          Column('working_hours_type', String(1)),
                          Column('hours_per_week_from', Integer),
                          Column('hours_per_week_to', Integer),
                          Column('employment_type', String(1)),
                          Column('full_text', Text),
                          Column('job_description', Text),
                          Column('candidate_description', Text),
                          Column('conditions_description', Text),
                          # Column('professional_skill_terms', Text),
                          Column('soft_skills', Text),
                          Column('professional_skills', Text),
                          Column('advertiser_house_number', String(15)),
                          Column('advertiser_email', String(255)),
                          Column('advertiser_website', String(255)),
                          Column('advertiser_contact_person', String(255)),
                          Column('advertiser_reference_number', String(255)),
                          Column('application_description', Text),
                          Column('organization_website', String(100)),
                          Column('employer_description', Text),
                          Column('language_skills', Text),
                          Column('it_skills', Text),
                          Column('organization_linkedin_id', String(255)),
                          Column('organization_national_id', String(25)),
                          Column('experience_years_from', Integer),
                          Column('salary', Integer),
                          Column('salary_from', Integer),
                          Column('salary_to', Integer),
                          Column('experience_years_to', Integer),
                          Column('advertiser_spend', Integer),
                          Column('apply_url', String(255)),
                          Column('experience_level', String(17)),
                          Column('location_postal_code', String(7)),
                          Column('profession_kldb_code', String(5)),
                          Column('profession_onet_2019_code', String(10)),
                          Column('salary_from_rate', String(10)),
                          Column('salary_time_scale', String(1)),
                          Column('salary_to_rate', String(10))
                          )

        metadata.create_all(engine)


    # [Start extract_s3]
    extract_s3_task = PythonOperator(
        task_id="download_s3_data",
        python_callable=extract_s3,
        dag=dag
    )
    # [End extract_s3]

    # [Start check_db]
    check_db_task = PythonOperator(
        task_id="check_db",
        python_callable=check_db,
        dag=dag
    )
    # [End check_db]

    # Read
    spark_job_extract = SparkSubmitOperator(
        task_id="extract_job",
        application="/opt/airflow/spark/jobs/spark_extract_job.py", # Spark application path created in airflow and spark cluster
        name="Load",
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":config['SPARK_MASTER']},
        application_args=[config['POSTGRES_CONNECTION_JDBC_STRING'],config['POSTGRES_USER'],
                          config['POSTGRES_PASSWORD'], "{{ ti.xcom_pull(task_ids='extract_s3') }}"],
        jars=config['POSTGRES_DRIVER_JAR'],
        driver_class_path=config['POSTGRES_DRIVER_JAR'],
        dag=dag
    )

    # Transform
    spark_job_transform = SparkSubmitOperator(
        task_id="transform_job",
        application="/opt/airflow/spark/jobs/spark_transform_job.py", # Spark application path created in airflow and spark cluster
        name="Transform",
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":config['SPARK_MASTER']},
        application_args=[],
        jars=config['POSTGRES_DRIVER_JAR'],
        driver_class_path=config['POSTGRES_DRIVER_JAR'],
        dag=dag
    )

    # Load
    spark_job_load = SparkSubmitOperator(
        task_id="load_job",
        application="/opt/airflow/spark/jobs/spark_load_job.py", # Spark application path created in airflow and spark cluster
        name="Load",
        conn_id="spark_default",
        verbose=1,
        conf={"spark.master":config['SPARK_MASTER']},
        application_args=[config['POSTGRES_CONNECTION_JDBC_STRING'],config['POSTGRES_USER'],
                          config['POSTGRES_PASSWORD'], config['POSTGRES_TABLE']],
        jars=config['POSTGRES_DRIVER_JAR'],
        driver_class_path=config['POSTGRES_DRIVER_JAR'],
        dag=dag
    )

    [extract_s3_task, check_db_task] >> spark_job_extract >> spark_job_transform >> spark_job_load