import io
import json
import boto3
import pandas as pd
from datetime import datetime, timedelta
from awsglue.utils import getResolvedOptions
from pyiceberg import IcebergTable

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

for event in events:
    cloudtrail_event = event['CloudTrailEvent']
    event_payload = json.loads(cloudtrail_event)['requestParameters']['eventPayload']

    # Check if the event ID matches the current batched event ID
    if "[{}]".format(event_payload['eventId']) == batched_events:
        print("Details:", event_payload['eventBody']['detail'])

        # Extract bucket name and object key
        bucket_name = event_payload['eventBody']['detail']['bucket']['name']
        object_key = event_payload['eventBody']['detail']['object']['key']

        # Download CSV file from S3
        obj = s3_client.get_object(Bucket=bucket_name, Key=object_key)

        # Read CSV file with pandas
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))

        # Create Iceberg table schema
        schema = {
            "TransactionID": "string",
            "CustomerID": "int",
            "Name": "string",
            "InvoiceNumber": "string",
            "InvoiceDate": "date",
            "InvoiceAmount": "double",
            "Currency": "string"
        }

        # Create Iceberg table
        iceberg_table = IcebergTable("s3://raw-data-coffee-905418260021/tables/account_receivable", schema)

        # Write data to Iceberg table
        iceberg_table.write(df, mode="overwrite")

        print("Data written to Iceberg table successfully.")
