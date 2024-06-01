import * as cdk from "aws-cdk-lib";
import { RemovalPolicy } from "aws-cdk-lib";
import * as events from "aws-cdk-lib/aws-events";
import * as targets from "aws-cdk-lib/aws-events-targets";
import * as glue from "aws-cdk-lib/aws-glue";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lakeformation from "aws-cdk-lib/aws-lakeformation";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { Construct } from "constructs";

export class GluePipelineStack extends cdk.Stack {
    constructor(scope: Construct, id: string, props?: cdk.StackProps) {
        super(scope, id, props);

        // Create raw, processed, and glue scripts S3 buckets
        const rawBucket = new s3.Bucket(this, "raw_data", {
            bucketName: `raw-data-coffee-${cdk.Aws.ACCOUNT_ID}`,
            autoDeleteObjects: true,
            removalPolicy: RemovalPolicy.DESTROY,
            lifecycleRules: [
                {
                    transitions: [
                        {
                            storageClass: s3.StorageClass.GLACIER,
                            transitionAfter: cdk.Duration.days(7),
                        },
                    ],
                },
            ],
            eventBridgeEnabled: true,
            enforceSSL: true,
            encryption: s3.BucketEncryption.S3_MANAGED,
        });

        const processedBucket = new s3.Bucket(this, "processed_data", {
            bucketName: `processed-data-coffee-${cdk.Aws.ACCOUNT_ID}`,
            autoDeleteObjects: true,
            removalPolicy: RemovalPolicy.DESTROY,
            enforceSSL: true,
            encryption: s3.BucketEncryption.S3_MANAGED,
        });

        const scriptsBucket = new s3.Bucket(this, "glue_scripts", {
            bucketName: `glue-scripts-${cdk.Aws.ACCOUNT_ID}`,
            autoDeleteObjects: true,
            removalPolicy: RemovalPolicy.DESTROY,
            enforceSSL: true,
            encryption: s3.BucketEncryption.S3_MANAGED,
        });

        const glueDatabaseBucket = new s3.Bucket(this, "glue_database", {
            bucketName: `glue-database-${cdk.Aws.ACCOUNT_ID}`,
            autoDeleteObjects: true,
            removalPolicy: RemovalPolicy.DESTROY,
            enforceSSL: true,
            encryption: s3.BucketEncryption.S3_MANAGED,
        });

        // Create SQS
        const glueQueue = new sqs.Queue(this, "glue_queue", {
            removalPolicy: RemovalPolicy.DESTROY, // Set removal policy
        });

        rawBucket.addEventNotification(
            s3.EventType.OBJECT_CREATED,
            new s3n.SqsDestination(glueQueue),
        );

        // Upload glue script to the specified bucket
        new s3deploy.BucketDeployment(this, "deployment", {
            sources: [s3deploy.Source.asset("./assets/")],
            destinationBucket: scriptsBucket,
        });

        // Create role for Glue Job and attach it
        const glueRole = new iam.Role(this, "glue_role", {
            roleName: "GlueRole",
            description: "Role for Glue services to access S3",
            assumedBy: new iam.ServicePrincipal("glue.amazonaws.com"),
            inlinePolicies: {
                glue_policy: new iam.PolicyDocument({
                    statements: [
                        new iam.PolicyStatement({
                            effect: iam.Effect.ALLOW,
                            actions: [
                                "s3:*",
                                "glue:*",
                                "iam:*",
                                "logs:*",
                                "cloudwatch:*",
                                "sqs:*",
                                "ec2:*",
                                "cloudtrail:*",
                            ],
                            resources: ["*"],
                        }),
                    ],
                }),
            },
        });

        glueRole.applyRemovalPolicy(RemovalPolicy.DESTROY);

        // Create Glue Database and grant LakeFormation permissions
        const glueDatabase = new glue.CfnDatabase(this, "tax_database", {
            catalogId: cdk.Aws.ACCOUNT_ID,
            databaseInput: {
                name: "tax_database",
                description: "Database to store tax data.",
                locationUri: glueDatabaseBucket.bucketArn,
            },
        });
        glueDatabase.applyRemovalPolicy(RemovalPolicy.DESTROY);

        const lakeformationPermission = new lakeformation.CfnPermissions(
            this,
            "lakeformation_permission",
            {
                dataLakePrincipal: {
                    dataLakePrincipalIdentifier: glueRole.roleArn,
                },
                resource: {
                    databaseResource: {
                        catalogId: glueDatabase.catalogId,
                        name: "tax_database",
                    },
                },
                permissions: ["ALL"],
            },
        );
        lakeformationPermission.applyRemovalPolicy(RemovalPolicy.DESTROY);

        lakeformationPermission.addDependency(glueDatabase);

        // Create Glue job
        const glueJob = new glue.CfnJob(this, "glue_job", {
            name: "glue_job",
            command: {
                name: "glueetl",
                pythonVersion: "3",
                scriptLocation: `s3://${scriptsBucket.bucketName}/glue_job.py`,
            },
            role: glueRole.roleArn,
            glueVersion: "3.0",
            timeout: 30,
        });
        glueJob.applyRemovalPolicy(RemovalPolicy.DESTROY);

        // Create Glue Workflow
        const glueWorkflow = new glue.CfnWorkflow(this, "glue_workflow", {
            name: "glue_workflow",
            description: "Workflow to process the coffee data.",
        });
        glueWorkflow.applyRemovalPolicy(RemovalPolicy.DESTROY);

        // Create Glue Job trigger
        const glueJobTrigger = new glue.CfnTrigger(this, "glue_job_trigger", {
            name: "glue_job_trigger",
            actions: [
                {
                    jobName: glueJob.name,
                    notificationProperty: { notifyDelayAfter: 3 },
                    timeout: 3,
                },
            ],
            type: "ON_DEMAND",
            workflowName: glueWorkflow.name,
        });
        glueJobTrigger.applyRemovalPolicy(RemovalPolicy.DESTROY);

        // Create EventBridge rule to trigger the Glue workflow when an object is created in the raw data bucket
        const ruleRole = new iam.Role(this, "rule_role", {
            roleName: "EventBridgeRole",
            description: "Role for EventBridge to trigger Glue workflows.",
            assumedBy: new iam.ServicePrincipal("events.amazonaws.com"),
            inlinePolicies: {
                eventbridge_policy: new iam.PolicyDocument({
                    statements: [
                        new iam.PolicyStatement({
                            effect: iam.Effect.ALLOW,
                            actions: ["events:*", "glue:*"],
                            resources: ["*"],
                        }),
                    ],
                }),
            },
        });
        ruleRole.applyRemovalPolicy(RemovalPolicy.DESTROY);

        const ruleS3Glue = new events.Rule(this, "rule_s3_glue", {
            eventPattern: {
                source: ["aws.s3"],
                detailType: ["Object Created"],
                detail: {
                    bucket: {
                        name: [rawBucket.bucketName],
                    },
                },
            },
            targets: [
                new targets.AwsApi({
                    service: "Glue",
                    action: "startWorkflowRun",
                    parameters: {
                        Name: glueWorkflow.name,
                    },
                    policyStatement: new iam.PolicyStatement({
                        actions: ["glue:StartWorkflowRun"],
                        resources: [
                            `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:workflow/${glueWorkflow.name}`,
                        ],
                    }),
                }),
            ],
        });
        ruleS3Glue.node.addDependency(ruleRole);

        // Define outputs
        new cdk.CfnOutput(this, "DatabaseNameOutput", {
            value: glueDatabase.ref,
            exportName: "DatabaseName",
        });

        new cdk.CfnOutput(this, "DatabaseBucketNameOutput", {
            value: glueDatabaseBucket.bucketName,
            exportName: "DatabaseBucketName",
        });

        new cdk.CfnOutput(this, "RawBucketNameOutput", {
            value: rawBucket.bucketName,
            exportName: "RawBucketName",
        });

        new cdk.CfnOutput(this, "ProcessedBucketNameOutput", {
            value: processedBucket.bucketName,
            exportName: "ProcessedBucketName",
        });

        new cdk.CfnOutput(this, "ScriptsBucketNameOutput", {
            value: scriptsBucket.bucketName,
            exportName: "ScriptsBucketName",
        });

        new cdk.CfnOutput(this, "GlueRoleArnOutput", {
            value: glueRole.roleArn,
            exportName: "GlueRoleArn",
        });
    }
}
