#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { Aspects } from 'aws-cdk-lib';
import { AwsSolutionsChecks } from 'cdk-nag';
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

// cdk-nag: AWS Solutions checks
Aspects.of(app).add(new AwsSolutionsChecks({ verbose: true }));
