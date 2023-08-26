from __future__ import annotations

# [START tutorial]
# [START import_module]
import json
from textwrap import dedent

import pendulum
import boto3
import gzip
import os
import logging
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Date, Boolean, Text, BigInteger
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python import PythonOperator

# [END import_module]


# spark_master = "spark://spark:7077"
config_file = "/opt/airflow/spark/assets/configs/config.json"
with open(config_file, 'r') as f:
    config = json.load(f)

# [START instantiate_dag]
with DAG(
        "etl_pipeline_dag",
        # [START default_args]
        # These args will get passed on to each operator
        # You can override them on a per-task basis during operator initialization
        default_args={"retries": 2},
        # [END default_args]
        description="DAG tutorial",
        schedule=None,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["example"],
) as dag:
    # [END instantiate_dag]
    # [START documentation]
    dag.doc_md = __doc__
    # [END documentation]

    # [START extract_function]
    def extract(**kwargs):
        ti = kwargs["ti"]

        # Load environment variables
        aws_access_key_id = "***REMOVED***"
        aws_secret_access_key = "***REMOVED***"
        aws_region = "eu-central-1"
        aws_bucket_name = "jobfeed-data-feeds"

        # Creating Boto3 Session
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )

        logging.info("S3 session created", session)
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

        # Create a connection to Postgres
        connection_string = "postgresql+psycopg2://airflow:airflow@postgres/job_ads_db"
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        print(engine)
        pgconn = engine.connect()
        print(pgconn)

        #Create spark session
        # spark = SparkSession.builder \
        #     .master("local") \
        #     .appName("DE-Project") \
        #     .getOrCreate()
        # # .config("spark.sql.shuffle.partitions", "50") \
        # # .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        # print(spark)
        # df = spark.read.json(filesToLoadInDF)
        # df.printSchema()


        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'
        ti.xcom_push("order_data", data_string)

    # [END extract_function]

    # [START transform_function]
    def transform(**kwargs):
        ti = kwargs["ti"]
        extract_data_string = ti.xcom_pull(task_ids="extract", key="order_data")
        order_data = json.loads(extract_data_string)

        total_order_value = 0
        for value in order_data.values():
            total_order_value += value

        total_value = {"total_order_value": total_order_value}
        total_value_json_string = json.dumps(total_value)
        ti.xcom_push("total_order_value", total_value_json_string)

    # [END transform_function]

    # [START load_function]
    def load(**kwargs):
        ti = kwargs["ti"]
        total_value_string = ti.xcom_pull(task_ids="transform", key="total_order_value")
        total_order_value = json.loads(total_value_string)

        print(total_order_value)

    # [END load_function]

    # running the spark job with spark submit operator
    # spark_job_extract = SparkSubmitOperator(
    # task_id="spark_transform_job",
    # application="/opt/airflow/spark/jobs/extract_app.py",
    # name="extract_app",
    # conn_id="spark_default",
    # verbose=1,
    # conf={"spark.master": config['SPARK_MASTER']},
    # py_files='/opt/airflow/spark/jobs/extract_job.py',
    # application_args=[config_file],
    # jars=config['POSTGRES_DRIVER_JAR'],
    # driver_class_path=config['POSTGRES_DRIVER_JAR'],
    # dag=dag)

    # Read
    spark_job_read_postgres = SparkSubmitOperator(
    task_id="spark_job_read_postgres",
    application="/opt/airflow/spark/jobs/spark_etl.py", # Spark application path created in airflow and spark cluster
    name="read-postgres",
    conn_id="spark_default",
    verbose=1,
    conf={"spark.master":config['SPARK_MASTER']},
    application_args=[config['POSTGRES_CONNECTION_JDBC_STRING'],config['POSTGRES_USER'],config['POSTGRES_PASSWORD']],
    jars=config['POSTGRES_DRIVER_JAR'],
    driver_class_path=config['POSTGRES_DRIVER_JAR'],
    dag=dag)


    # [START main_flow]
    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )
    extract_task.doc_md = dedent(
        """\
    #### Extract task
    A simple Extract task to get data ready for the rest of the data pipeline.
    In this case, getting data is simulated by reading from a hardcoded JSON string.
    This data is then put into xcom, so that it can be processed by the next task.
    """
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )
    transform_task.doc_md = dedent(
        """\
    #### Transform task
    A simple Transform task which takes in the collection of order data from xcom
    and computes the total order value.
    This computed value is then put into xcom, so that it can be processed by the next task.
    """
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load,
    )
    load_task.doc_md = dedent(
        """\
    #### Load task
    A simple Load task which takes in the result of the Transform task, by reading it
    from xcom and instead of saving it to end user review, just prints it out.
    """
    )

    extract_task >> transform_task >> load_task

# [END main_flow]

# [END tutorial]
