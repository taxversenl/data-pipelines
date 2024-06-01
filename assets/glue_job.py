import sys
import json
import boto3
import pandas as pd
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession

# Initialize Glue client
glue_client = boto3.client("glue")

# Get resolved options
queue_url = 'https://sqs.eu-west-1.amazonaws.com/905418260021/GluePipelineStack-gluequeue6D2CCD0B-499n4FzYiEBh'

# Initialize SQS client
sqs_client = boto3.client('sqs')

# Receive messages from SQS queue
response = sqs_client.receive_message(
    QueueUrl=queue_url,
    MaxNumberOfMessages=10,  # Adjust as needed
    WaitTimeSeconds=20  # Long polling
)

messages = response.get("Messages", [])
print(f"Received {len(messages)} messages from SQS.")

if not messages:
    print("No messages received from SQS.")
    sys.exit(0)

print(f"Received {len(messages)} messages from SQS.")

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('data-review-service-DatasetTable-118ZWIA21PX1U')

# Process each SQS message
for message in messages:
    print(f"Processing message: {message}")
    body = json.loads(message['Body'])
    event_records = body.get('Records', [])
    
    for record in event_records:
        s3_info = record.get('s3', {})
        bucket_name = s3_info.get('bucket', {}).get('name', '')
        object_key = s3_info.get('object', {}).get('key', '')

        if bucket_name and object_key:
            try:
                print(f"Processing S3 object: Bucket - {bucket_name}, Key - {object_key}")

                # Initialize SparkSession
                spark = SparkSession.builder \
                    .appName("Read CSV from S3") \
                    .getOrCreate()

                # Specify the path to the CSV file in S3 using the extracted bucket name and object key
                s3_path = f"s3://{bucket_name}/{object_key}"
                print(f"Reading CSV from S3 path: {s3_path}")

                # Read the CSV file into a DataFrame
                df = spark.read.csv(s3_path, header=True, inferSchema=True)


                # Convert all columns to string
                for col in df.columns:
                    df = df.withColumn(col, df[col].cast("string"))

                # Convert Spark DataFrame to Pandas DataFrame for ease of writing to DynamoDB
                pandas_df = df.toPandas()

                # Function to create composite key
                def create_composite_key(row):
                    dataset_type = "AR"  # Hardcoded value
                    reporting_period = row['reporting_period'][:7]  # Assuming 'invoicedate' is in 'YYYY-MM-DD' format
                    transaction_id = row['transaction_id']
                    return f"{dataset_type}#{reporting_period}#{transaction_id}"

                # Write each row to DynamoDB
                with table.batch_writer() as batch:
                    for index, row in pandas_df.iterrows():
                        item = {
                            'registration_id': row['tax_registration_id'],
                            'dataset_type#reporting_period#transaction_id': create_composite_key(row)
                        }
                        for k, v in row.items():
                            if pd.notnull(v):
                                item[k] = v
                        batch.put_item(Item=item)

                # Delete the message from the queue after processing
                sqs_client.delete_message(
                    QueueUrl=queue_url,
                    ReceiptHandle=message['ReceiptHandle']
                )
                print(f"Deleted message with ReceiptHandle: {message['ReceiptHandle']}")

            except Exception as e:
                print(f"Error processing file {object_key} from bucket {bucket_name}: {str(e)}")
        else:
            print("Error: Unable to extract bucket name or object key from SQS messages.")
