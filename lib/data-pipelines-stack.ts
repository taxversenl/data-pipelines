import * as cdk from "aws-cdk-lib";
import { Stack } from "aws-cdk-lib";
import { Construct } from "constructs";

import { GluePipelineStack } from "../bin/glue-pipeline-stack";
import { IcebergTable } from "../bin/iceberg-table";
import { ServerlessPostgresStack } from "../bin/serverless-postgress-stack";

export class DataPipelinesStack extends Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, { ...props, crossRegionReferences: true });

        new ServerlessPostgresStack(scope, "ServerlessPostgresStack", {});
        new GluePipelineStack(scope, "GluePipelineStack", {});
        // Define parameters for the Iceberg table
        const databaseName = cdk.Fn.importValue("DatabaseName");
        const bucketName = cdk.Fn.importValue("RawBucketName");
        const tableName = "account_receivable"; // Corrected table name
        const columns = `\
        TransactionID string,\
        CustomerID int,\
        Name string,\
        InvoiceNumber string,\
        InvoiceDate date,\
        InvoiceAmount double,\
        Currency string`;
        const partitionedBy = ""; // No partitioning specified in the template
        const workgroup = "primary";

        // Create the Iceberg table
        new IcebergTable(this, "account_receivables", {
            databaseName,
            tableName,
            columns,
            partitionedBy,
            S3Bucket: bucketName,
            workgroup,
        });
    }
}
