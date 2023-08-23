import os
import boto3
import init_session
import gzip
import json
from io import BytesIO
from pyspark.sql import SparkSession

"""Initialize Session with Server"""
session = init_session.session

# Create an S3 client
s3 = session.client('s3')

bucket_name = "jobfeed-data-feeds"
prefix = 'DE/monthly/'

# Get the list of objects in the S3 bucket
response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix, Delimiter='/')
# Get the list of subfolders in the S3 bucket
subfolders = [obj['Prefix'] for obj in response['CommonPrefixes']]
# Get the last N subfolders
subfolders = subfolders[-6:]

filesToLoadInDF = []
# Download files from each subfolder
for subfolder in subfolders:
    # Get the list of files in the subfolder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=subfolder)
    # Get the file paths
    files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.jsonl.gz')]
    # Only get the first N files
    # files = files[:1]

    # filesToLoadInDF = [filesToLoadInDF.append(f) for f in files]

    # Create the folder in your local machine
    folder = bucket_name + "/" + subfolder
    if not os.path.exists(folder):
        os.makedirs(folder)
    # Download and extract each file
    for file in files:
        filename = file.rsplit("/", 1)[1]
        print('Downloading file {}...'.format(filename))
        print(subfolder + filename)
        print(folder + filename)

        # Download and Save the file
        s3.download_file(Filename=folder + filename, Bucket=bucket_name, Key=subfolder + filename)

        locaFilePath = os.path.join(folder + filename)
        localExtractedFilePath = os.path.join(folder + filename[:-3])
        print(localExtractedFilePath)
        filesToLoadInDF.append(localExtractedFilePath)
        # Extract the data from the gzipped file
        with gzip.open(locaFilePath, 'rb') as gz_file, open(localExtractedFilePath, 'wb') as extract_file:
            extract_file.write(gz_file.read())

        # Delete the gzipped file
        # os.remove(locaFilePath)

# Step 2: Load the extracted data into a DataFrame using PySpark
spark = SparkSession.builder.getOrCreate()
print(filesToLoadInDF)
df = spark.read.json(filesToLoadInDF)  # Use the extracted file paths here
df.show()
