import * as cdk from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { CONFIG } from './config';

export class MskConnectIamStack extends cdk.Stack {
  public readonly serviceRoleArn: string;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // The Debezium plugin bucket is created manually by the user (or they can
    // leverage an existing metadata management bucket). The bucket ARN is
    // referenced here for the S3 read policy on the MSK Connect service role.
    const pluginBucketArn = `arn:aws:s3:::${CONFIG.debeziumPluginBucket}`;

    const role = new iam.Role(this, 'ServiceRole', {
      roleName: 'msk-connect-service-role',
      assumedBy: new iam.ServicePrincipal('kafkaconnect.amazonaws.com'),
      managedPolicies: [iam.ManagedPolicy.fromAwsManagedPolicyName('CloudWatchLogsFullAccess')],
    });

    role.addToPolicy(new iam.PolicyStatement({
      actions: [
        'kafka-cluster:Connect', 'kafka-cluster:AlterCluster', 'kafka-cluster:DescribeCluster',
        'kafka-cluster:WriteData', 'kafka-cluster:ReadData',
        'kafka-cluster:CreateTopic', 'kafka-cluster:DescribeTopic', 'kafka-cluster:AlterTopic',
      ],
      resources: ['*'],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['ec2:CreateNetworkInterface', 'ec2:DescribeNetworkInterfaces', 'ec2:DeleteNetworkInterface', 'ec2:DescribeSubnets', 'ec2:DescribeSecurityGroups', 'ec2:DescribeVpcs'],
      resources: ['*'],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:GetObject', 's3:ListBucket'],
      resources: [pluginBucketArn, `${pluginBucketArn}/*`],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['secretsmanager:GetSecretValue', 'secretsmanager:DescribeSecret'],
      resources: [CONFIG.auroraSecretArn],
    }));

    new logs.LogGroup(this, 'ConnectorLogs', {
      logGroupName: '/aws/msk-connect/aurora-cdc-debezium',
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    this.serviceRoleArn = role.roleArn;
    new cdk.CfnOutput(this, 'ServiceRoleArn', { value: role.roleArn });
  }
}
