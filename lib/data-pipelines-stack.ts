import * as cdk from "aws-cdk-lib";
import { Stack } from "aws-cdk-lib";
import { Construct } from "constructs";

import { GluePipelineStack } from "../bin/glue-pipeline-stack";
import { ServerlessPostgresStack } from "../bin/serverless-postgress-stack";

export class DataPipelinesStack extends Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, { ...props, crossRegionReferences: true });

        new ServerlessPostgresStack(scope, "ServerlessPostgresStack", {});
        new GluePipelineStack(scope, "GluePipelineStack", {});
    }
}
