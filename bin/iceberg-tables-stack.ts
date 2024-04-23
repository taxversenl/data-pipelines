import { RemovalPolicy, Stack } from "aws-cdk-lib";
import * as iam from "aws-cdk-lib/aws-iam";
import * as logs from "aws-cdk-lib/aws-logs";
import * as s3 from "aws-cdk-lib/aws-s3";
import { AwsCustomResource, PhysicalResourceId } from "aws-cdk-lib/custom-resources";
import { Construct } from "constructs";

export class IcebergTablesStack extends Stack {
    constructor(scope: Construct, id: string, props: IcebergTableProps) {
        super(scope, id, { ...props, crossRegionReferences: true });

        const { databaseName, tableName, columns, partitionedBy, S3Bucket, workgroup } = props;

        new s3.Bucket(this, "table_data", {
            bucketName: `${S3Bucket}`,
            autoDeleteObjects: true,
            removalPolicy: RemovalPolicy.DESTROY,
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
                    )} LOCATION 's3://${S3Bucket}/tables/${tableName}' TBLPROPERTIES ('table_type'='iceberg');`,
                    ResultConfiguration: {
                        OutputLocation: `s3://${S3Bucket}/athena_temp/`,
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
                        OutputLocation: `s3://${S3Bucket}/athena_temp/`,
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

interface IcebergTableProps {
    databaseName: string;
    tableName: string;
    columns: string;
    partitionedBy: string;
    S3Bucket: string;
    workgroup: string;
}
