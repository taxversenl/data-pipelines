import * as cdk from "aws-cdk-lib";
import { RemovalPolicy, Stack } from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import * as s3 from "aws-cdk-lib/aws-s3";
import { AwsCustomResource, PhysicalResourceId } from "aws-cdk-lib/custom-resources";
import { Construct } from "constructs";

export class IcebergTablesStack extends Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, { ...props, crossRegionReferences: true });

        // Define parameters for the Iceberg table
        const catalogBucketName = cdk.Fn.importValue("DatabaseBucketName");

        // Define parameters for the Iceberg table
        const databaseName = cdk.Fn.importValue("DatabaseName");
        const tableName = "account_receivable";
        const columns = `\
          TransactionID string,\
          CustomerID int,\
          Name string,\
          InvoiceNumber string,\
          InvoiceDate string,\
          InvoiceAmount double,\
          Currency string,\
          TaxRegistrationID string,\
          LegalEntity string,\
          Region string,\
          Country string,\
          BusinessLine string,\
          LockPeriod string`;
        const partitionedBy = "lockperiod"; // Partitioned by LockPeriod
        const workgroup = "primary";

        const athenaOuputBucket = new s3.Bucket(this, "athena-output", {
            bucketName: `athena-logs-${cdk.Aws.ACCOUNT_ID}`,
            autoDeleteObjects: true,
            removalPolicy: RemovalPolicy.DESTROY,
            enforceSSL: true,
            encryption: s3.BucketEncryption.S3_MANAGED,
        });

        new AwsCustomResource(this, `IcebergTableCustomResource-${tableName}`, {
            installLatestAwsSdk: false,
            role: new iam.Role(this, `IcebergTableLambdaRole-${tableName}`, {
                assumedBy: new iam.ServicePrincipal("lambda.amazonaws.com"),
                managedPolicies: [
                    iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonS3FullAccess"),
                    iam.ManagedPolicy.fromAwsManagedPolicyName("AmazonAthenaFullAccess"),
                ],
            }),
            logRetention: logs.RetentionDays.INFINITE,
            onCreate: {
                service: "Athena",
                action: "startQueryExecution",
                parameters: {
                    QueryExecutionContext: {
                        Database: databaseName,
                    },
                    QueryString: `CREATE TABLE ${tableName} (${columns})${IcebergTablesStack.getPartitionedBy(
                        partitionedBy,
                    )} LOCATION 's3://${catalogBucketName}/tables/${tableName}' TBLPROPERTIES ('table_type'='iceberg');`,
                    ResultConfiguration: {
                        OutputLocation: `s3://${athenaOuputBucket.bucketName}/athena_temp/`,
                    },
                    WorkGroup: workgroup,
                },
                physicalResourceId: PhysicalResourceId.of(`IcebergTable-${tableName}`),
            },
            onDelete: {
                service: "Athena",
                action: "startQueryExecution",
                parameters: {
                    QueryExecutionContext: {
                        Database: databaseName,
                    },
                    QueryString: `DROP TABLE ${tableName}`,
                    ResultConfiguration: {
                        OutputLocation: `s3://${athenaOuputBucket}/athena_temp/`,
                    },
                    WorkGroup: workgroup,
                },
                physicalResourceId: PhysicalResourceId.of(`IcebergTable-${tableName}`),
            },
            resourceType: "Custom::CustomResourcesIcebergTable",
        });
    }

    private static getPartitionedBy(partitionedBy: string): string {
        return partitionedBy.length > 0 ? ` PARTITIONED BY (${partitionedBy}) ` : "";
    }
}
