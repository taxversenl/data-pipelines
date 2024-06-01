import sys
import io
import json
import boto3
import pandas as pd
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

# Initialize Glue client
glue_client = boto3.client("glue")

# Get resolved options
args = getResolvedOptions(sys.argv, ['WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
workflow_name = args['WORKFLOW_NAME']
workflow_run_id = args['WORKFLOW_RUN_ID']

# Get workflow run properties
workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name, RunId=workflow_run_id)["RunProperties"]
batched_events = workflow_params.get('aws:eventIds', '')  # Check if key exists
print("Batched Events:", batched_events)

# Initialize CloudTrail client with Ireland (eu-west-1) region
cloudtrail_client = boto3.client('cloudtrail', region_name='eu-west-1')

# Initialize S3 client
s3_client = boto3.client('s3')

# Lookup CloudTrail events
response = cloudtrail_client.lookup_events(
    LookupAttributes=[
        {
            'AttributeKey': 'EventName',
            'AttributeValue': 'NotifyEvent'
        },
    ],
    StartTime=(datetime.now() - timedelta(minutes=10)),
    EndTime=datetime.now(),
    MaxResults=100
)

events = response.get("Events", [])

bucket_name ="";
object_key = "";

for event in events:
    cloudtrail_event = event['CloudTrailEvent']
    event_payload = json.loads(cloudtrail_event)['requestParameters']['eventPayload']

    # Check if the event ID matches the current batched event ID
    if "[{}]".format(event_payload['eventId']) == batched_events:
        print("Details:", event_payload['eventBody']['detail'])

        # Extract bucket name and object key
        bucket_name = event_payload['eventBody']['detail']['bucket']['name']
        object_key = event_payload['eventBody']['detail']['object']['key']

# Check if both bucket name and object key are non-empty
if bucket_name and object_key:
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("Read CSV from S3") \
        .getOrCreate()

    # Specify the path to the CSV file in S3 using the extracted bucket name and object key
    s3_path = f"s3://{bucket_name}/{object_key}"

    # Read the CSV file into a DataFrame
    df = spark.read.csv(s3_path, header=True, inferSchema=True)

    # Define the schema
    schema = {
        "transactionid": "string",
        "customerid": "int",
        "name": "string",
        "invoicenumber": "string",
        "invoicedate": "string",
        "invoiceamount": "double",
        "currency": "string",
        "taxregistrationid": "string",
        "legalentity": "string",
        "region": "string",
        "country": "string",
        "businessline": "string",
        "lockperiod": "string"
    }

    # Rename the columns and cast the data types
    df = df.toDF(*schema)

    # Show the DataFrame
    df.show()

    # Write DataFrame to Iceberg table
    catalog_name = "glue_catalog"
    database_name = "tax_database"
    table_name = "account_receivable"
    df.writeTo(f"{catalog_name}.{database_name}.{table_name}").append()

    # Show the Iceberg table and its history
    spark.table(f"{catalog_name}.{database_name}.{table_name}").show()
    spark.table(f"{catalog_name}.{database_name}.{table_name}.history").show()
else:
    print("Error: Unable to extract bucket name or object key from CloudTrail events.")
