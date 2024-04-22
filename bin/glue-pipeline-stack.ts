import * as cdk from "aws-cdk-lib";
import { RemovalPolicy } from "aws-cdk-lib";
import * as events from "aws-cdk-lib/aws-events";
import * as glue from "aws-cdk-lib/aws-glue";
import * as iam from "aws-cdk-lib/aws-iam";
import * as lakeformation from "aws-cdk-lib/aws-lakeformation";
import * as s3 from "aws-cdk-lib/aws-s3";
import * as s3deploy from "aws-cdk-lib/aws-s3-deployment";
import * as s3n from "aws-cdk-lib/aws-s3-notifications";
import * as sqs from "aws-cdk-lib/aws-sqs";
import { Construct } from "constructs";

export class GluePipelineStack extends cdk.Stack {
    constructor(scope: Construct, constructId: string, props?: cdk.StackProps) {
        super(scope, constructId, props);

        // Create raw, processed, and glue scripts S3 buckets
        const rawBucket = new s3.Bucket(this, "raw_data", {
            bucketName: `raw-data-coffee-${cdk.Aws.ACCOUNT_ID}`,
            autoDeleteObjects: true,
            removalPolicy: RemovalPolicy.DESTROY, // Set removal policy
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
        });

        const processedBucket = new s3.Bucket(this, "processed_data", {
            bucketName: `processed-data-coffee-${cdk.Aws.ACCOUNT_ID}`,
            autoDeleteObjects: true,
            removalPolicy: RemovalPolicy.DESTROY, // Set removal policy
        });

        const scriptsBucket = new s3.Bucket(this, "glue_scripts", {
            bucketName: `glue-scripts-${cdk.Aws.ACCOUNT_ID}`,
            autoDeleteObjects: true,
            removalPolicy: RemovalPolicy.DESTROY, // Set removal policy
        });

        // Upload glue script to the specified bucket
        new s3deploy.BucketDeployment(this, "deployment", {
            sources: [s3deploy.Source.asset("./assets/")],
            destinationBucket: scriptsBucket,
        });

        // Create SQS
        const glueQueue = new sqs.Queue(this, "glue_queue", {
            removalPolicy: RemovalPolicy.DESTROY, // Set removal policy
        });

        rawBucket.addEventNotification(
            s3.EventType.OBJECT_CREATED,
            new s3n.SqsDestination(glueQueue),
        );

        // Create role for Glue Crawler and Glue Job and attach it to them
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
        const glueDatabase = new glue.CfnDatabase(this, "glue-database", {
            catalogId: cdk.Aws.ACCOUNT_ID,
            databaseInput: {
                name: "tax-database",
                description: "Database to store tax data.",
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
                        name: "tax-database",
                    },
                },
                permissions: ["ALL"],
            },
        );
        lakeformationPermission.applyRemovalPolicy(RemovalPolicy.DESTROY);

        // Define outputs
        new cdk.CfnOutput(this, "DatabaseName", {
            value: glueDatabase.ref,
        });

        new cdk.CfnOutput(this, "RawBucketName", {
            value: rawBucket.bucketName,
        });

        new cdk.CfnOutput(this, "ProcessedBucketNameOutput", {
            value: processedBucket.bucketName,
        });

        new cdk.CfnOutput(this, "ScriptsBucketNameOutput", {
            value: scriptsBucket.bucketName,
        });

        // Create Glue Crawler
        const glueCrawler = new glue.CfnCrawler(this, "glue_crawler", {
            name: "glue_crawler",
            role: glueRole.roleArn,
            databaseName: "tax-database",
            targets: {
                s3Targets: [
                    {
                        path: `s3://${rawBucket.bucketName}/`,
                        eventQueueArn: glueQueue.queueArn,
                    },
                ],
            },
            recrawlPolicy: {
                recrawlBehavior: "CRAWL_EVENT_MODE",
            },
        });

        glueCrawler.applyRemovalPolicy(RemovalPolicy.DESTROY);

        // Create Glue job
        const glue_job = new glue.CfnJob(this, "glue_job", {
            name: "glue_job",
            command: {
                name: "pythonshell",
                pythonVersion: "3.9",
                scriptLocation: `s3://${scriptsBucket.bucketName}/glue_job.py`,
            },
            role: glueRole.roleArn,
            glueVersion: "3.0",
            timeout: 3,
        });
        glue_job.applyRemovalPolicy(RemovalPolicy.DESTROY);

        // Create Glue Workflow
        const glue_workflow = new glue.CfnWorkflow(this, "glue_workflow", {
            name: "glue_workflow",
            description: "Workflow to process the coffee data.",
        });
        glue_workflow.applyRemovalPolicy(RemovalPolicy.DESTROY);

        // Create Glue Job trigger
        const glue_job_trigger = new glue.CfnTrigger(this, "glue_job_trigger", {
            name: "glue_job_trigger",
            actions: [
                {
                    jobName: "glue_job",
                    notificationProperty: { notifyDelayAfter: 3 },
                    timeout: 3,
                },
            ],
            type: "CONDITIONAL",
            startOnCreation: true,
            workflowName: glue_workflow.name,
            predicate: {
                conditions: [
                    {
                        crawlerName: "glue_crawler",
                        logicalOperator: "EQUALS",
                        crawlState: "SUCCEEDED",
                    },
                ],
            },
        });
        glue_job_trigger.applyRemovalPolicy(RemovalPolicy.DESTROY);
        // Create Glue Crawler trigger
        const glue_crawler_trigger = new glue.CfnTrigger(this, "glue_crawler_trigger", {
            name: "glue_crawler_trigger",
            actions: [
                {
                    crawlerName: "glue_crawler",
                    notificationProperty: { notifyDelayAfter: 3 },
                    timeout: 3,
                },
            ],
            type: "EVENT",
            workflowName: glue_workflow.name,
        });
        glue_crawler_trigger.applyRemovalPolicy(RemovalPolicy.DESTROY);

        // Create EventBridge rule to trigger crawler based on upload events and role for it
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

        // Create an EventBridge rule to trigger the Glue crawler when an object is created
        const rule_s3_glue = new events.CfnRule(this, "rule_s3_glue", {
            name: "rule_s3_glue",
            roleArn: ruleRole.roleArn,
            targets: [
                {
                    arn: `arn:aws:glue:${cdk.Aws.REGION}:${cdk.Aws.ACCOUNT_ID}:workflow/glue_workflow`,
                    roleArn: ruleRole.roleArn,
                    id: cdk.Aws.ACCOUNT_ID,
                },
            ],
            eventPattern: {
                "detail-type": ["Object Created"],
                detail: {
                    bucket: { name: [rawBucket.bucketName] },
                },
                source: ["aws.s3"],
            },
        });
        rule_s3_glue.addDeletionOverride(RemovalPolicy.DESTROY);
    }
}
