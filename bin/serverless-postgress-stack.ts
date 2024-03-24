import {
  CfnOutput,
  Duration,
  RemovalPolicy,
  Stack,
  StackProps
} from "aws-cdk-lib";
import { AuroraCapacityUnit, DatabaseClusterEngine, ParameterGroup, ServerlessCluster } from "aws-cdk-lib/aws-rds";
import { Construct } from "constructs";


export class ServerlessPostgresStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, { ...props, crossRegionReferences: true });

        // Create a serverless PostgreSQL instance
        const database = new ServerlessCluster(scope, "TaxVerseDB", {
          engine: DatabaseClusterEngine.AURORA_POSTGRESQL,
          parameterGroup: ParameterGroup.fromParameterGroupName(
              this,
              "ParameterGroup",
              "default.aurora-postgresql16",
          ),
          scaling: {
              autoPause: Duration.minutes(5), // Pause the database after 15 minutes of inactivity
              minCapacity: AuroraCapacityUnit.ACU_1, // Minimum capacity
              maxCapacity: AuroraCapacityUnit.ACU_2, // Maximum capacity
          },
          enableDataApi: true, // Enable the Data API
          backupRetention: Duration.days(7), // Customize backup retention policy
          removalPolicy: RemovalPolicy.DESTROY, // WARNING: This will destroy your database on stack deletion
      });

      // Output the connection endpoint
      new CfnOutput(this, "DatabaseEndpoint", {
          value: database.clusterEndpoint.hostname,
          exportName: "DatabaseEndpoint",
      });
  }
}
