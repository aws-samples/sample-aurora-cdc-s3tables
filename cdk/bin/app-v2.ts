#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { Aspects } from 'aws-cdk-lib';
import { AwsSolutionsChecks, NagSuppressions } from 'cdk-nag';
import { MskClusterStack } from '../lib/v2/1-msk-cluster-stack';
import { MskConnectIamStack } from '../lib/v2/2-msk-connect-iam-stack';
import { DebeziumConnectorStack } from '../lib/v2/2b-debezium-connector-stack';
import { S3TablesStack } from '../lib/v2/3-s3-tables-stack';
import { LambdaTransformStack } from '../lib/v2/4-lambda-transform-stack';
import { FirehoseRoleStack } from '../lib/v2/5-firehose-role-stack';
import { LakeFormationStack } from '../lib/v2/6-lakeformation-stack';
import { FirehoseStack } from '../lib/v2/7-firehose-stack';
import { CONFIG } from '../lib/v2/config';

const app = new cdk.App();
const env = { account: CONFIG.account, region: CONFIG.region };

// 1. MSK Cluster (with IAM auth, VPC IAM connectivity, auto.create.topics.enable)
const msk = new MskClusterStack(app, 'CdcMskCluster', { env });

// 2a. MSK Connect IAM (role, plugin bucket, logs)
const mskIam = new MskConnectIamStack(app, 'CdcMskConnectIam', { env });

// 2b. Debezium Connector (MSK Connect)
//     NOTE: customPluginArn and workerConfigArn must be created first via CLI
//     (build-debezium-plugin.sh), then passed here. These are not CDK-managed
//     because the plugin ZIP must be built and uploaded to S3 manually.
const debezium = new DebeziumConnectorStack(app, 'CdcDebeziumConnector', {
  env,
  mskClusterArn: msk.clusterArn,
  mskBootstrapServers: CONFIG.mskBootstrapServers,
  mskSecurityGroupId: msk.securityGroupId,
  serviceRoleArn: mskIam.serviceRoleArn,
  customPluginArn: CONFIG.debeziumPluginArn,
  workerConfigArn: CONFIG.debeziumWorkerConfigArn,
});
debezium.addDependency(msk);
debezium.addDependency(mskIam);

// 3. S3 Tables (bucket, namespace, 3 Iceberg tables)
const s3Tables = new S3TablesStack(app, 'CdcS3Tables', { env });

// 4. Lambda Transform
const lambda = new LambdaTransformStack(app, 'CdcLambdaTransform', { env });

// 5. Firehose IAM Role
const firehoseRole = new FirehoseRoleStack(app, 'CdcFirehoseRole', {
  env,
  mskClusterArn: msk.clusterArn,
  lambdaArn: lambda.functionArn,
});
firehoseRole.addDependency(msk);
firehoseRole.addDependency(lambda);

// 6. Lake Formation + MSK cluster resource policy
const lf = new LakeFormationStack(app, 'CdcLakeFormation', {
  env,
  firehoseRoleArn: firehoseRole.roleArn,
  mskClusterArn: msk.clusterArn,
});
lf.addDependency(firehoseRole);
lf.addDependency(s3Tables);

// 7. Firehose delivery stream (MSK → S3 Tables)
const firehose = new FirehoseStack(app, 'CdcFirehose', {
  env,
  roleArn: firehoseRole.roleArn,
  mskClusterArn: msk.clusterArn,
  lambdaArn: lambda.functionArn,
});
firehose.addDependency(lf);

// ---- cdk-nag suppressions with justifications ----

// CdcMskCluster: TLS_PLAINTEXT required for MSK Connect (PLAINTEXT) + Firehose (TLS/IAM)
// Ref: https://docs.aws.amazon.com/msk/latest/developerguide/mkc-tutorial-setup.html
NagSuppressions.addStackSuppressions(msk, [
  { id: 'AwsSolutions-MSK2', reason: 'TLS_PLAINTEXT required: MSK Connect workers use PLAINTEXT protocol. Firehose uses TLS via IAM. Inter-broker encryption (InCluster: true) is enabled.' },
]);

// CdcMskConnectIam: Plugin bucket, managed policy, wildcard permissions
NagSuppressions.addStackSuppressions(mskIam, [
  { id: 'AwsSolutions-S1', reason: 'Access logging requires a separate bucket. Plugin bucket has SSE, versioning, and public access blocked.' },
  { id: 'AwsSolutions-S10', reason: 'SSL-only bucket policy should be added. Acceptable for sample code.' },
  { id: 'AwsSolutions-IAM4', reason: 'CloudWatchLogsFullAccess used for MSK Connect log delivery. Scoped custom policy recommended for production.', appliesTo: ['Policy::arn:<AWS::Partition>:iam::aws:policy/CloudWatchLogsFullAccess'] },
  { id: 'AwsSolutions-IAM5', reason: 'MSK Connect requires kafka-cluster:* and ec2:* with Resource:* for dynamic topic creation and VPC networking. Role scoped to kafkaconnect.amazonaws.com service principal.', appliesTo: ['Resource::*', 'Resource::<PluginBucket4953ED6D.Arn>/*'] },
]);

// CdcLambdaTransform: Managed policy
NagSuppressions.addStackSuppressions(lambda, [
  { id: 'AwsSolutions-IAM4', reason: 'AWSLambdaBasicExecutionRole grants only CloudWatch Logs permissions. AWS-recommended minimum for Lambda.', appliesTo: ['Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'] },
]);

// CdcFirehoseRole: Wildcard permissions required by Firehose for Glue, LakeFormation, S3, EC2, MSK
// Ref: https://docs.aws.amazon.com/firehose/latest/dev/controlling-access.html#using-s3-tables
NagSuppressions.addStackSuppressions(firehoseRole, [
  { id: 'AwsSolutions-IAM5', reason: 'Firehose IAM role requires broad permissions for Glue (database/*), LakeFormation (GetDataAccess), S3 (Iceberg data files), EC2 (VPC networking), and MSK (dynamic topics/groups). AWS docs prescribe these wildcards. Role scoped to firehose.amazonaws.com.', appliesTo: [
    'Resource::*',
    'Resource::arn:aws:logs:<AWS::AccountId>:log-group:/aws/kinesisfirehose/*',
    'Resource::arn:aws:s3tables:<AWS::AccountId>:bucket/<BucketName>/*',
    'Resource::arn:aws:kafka:<AWS::AccountId>:topic/*',
    'Resource::arn:aws:kafka:<AWS::AccountId>:group/*',
  ]},
], true);

// CdcLakeFormation: Custom resource wildcards and managed policy
NagSuppressions.addStackSuppressions(lf, [
  { id: 'AwsSolutions-IAM4', reason: 'AwsCustomResource Lambda uses AWSLambdaBasicExecutionRole for CloudWatch Logs.', appliesTo: ['Policy::arn:<AWS::Partition>:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole'] },
  { id: 'AwsSolutions-IAM5', reason: 'AwsCustomResource requires Resource:* for LakeFormation and Kafka SDK calls.' },
  { id: 'AwsSolutions-L1', reason: 'AwsCustomResource Lambda runtime is managed by CDK and cannot be overridden.' },
]);

// CdcFirehose: SSE not applicable for MSK-sourced streams
// Ref: https://docs.aws.amazon.com/firehose/latest/dev/encryption.html
NagSuppressions.addStackSuppressions(firehose, [
  { id: 'AwsSolutions-KDF1', reason: 'Firehose SSE applies to Direct PUT sources. With MSK source, data is encrypted in transit via TLS. Firehose encrypts interim storage using AWS KMS automatically.' },
]);

// cdk-nag: AWS Solutions checks
Aspects.of(app).add(new AwsSolutionsChecks({ verbose: true }));
