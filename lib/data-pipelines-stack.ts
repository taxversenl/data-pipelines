import * as cdk from "aws-cdk-lib";
import { Construct } from "constructs";
import { ServerlessPostgresStack } from "../bin/serverless-postgress-stack";

export class DataPipelinesStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    new ServerlessPostgresStack(scope,'ServerlessRDSStack',{});
  }
}
