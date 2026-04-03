import * as cdk from 'aws-cdk-lib';
import * as iam from 'aws-cdk-lib/aws-iam';
import { Construct } from 'constructs';
import { CONFIG } from './config';

interface Props extends cdk.StackProps {
  mskClusterArn: string;
  lambdaArn: string;
}

export class FirehoseRoleStack extends cdk.Stack {
  public readonly roleArn: string;

  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id, props);

    const mskClusterName = CONFIG.mskClusterName;

    const role = new iam.Role(this, 'Role', {
      roleName: 'firehose-msk-s3tables-role',
      assumedBy: new iam.ServicePrincipal('firehose.amazonaws.com'),
    });

    role.addToPolicy(new iam.PolicyStatement({
      actions: ['kafka:GetBootstrapBrokers', 'kafka:DescribeCluster', 'kafka:DescribeClusterV2', 'kafka:CreateVpcConnection'],
      resources: [props.mskClusterArn],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: [
        'kafka-cluster:Connect', 'kafka-cluster:DescribeCluster', 'kafka-cluster:DescribeTopic',
        'kafka-cluster:DescribeTopicDynamicConfiguration', 'kafka-cluster:ReadData',
        'kafka-cluster:DescribeGroup', 'kafka-cluster:AlterGroup',
        'kafka-cluster:CreateTopic', 'kafka-cluster:WriteData',
      ],
      resources: [
        props.mskClusterArn,
        `arn:aws:kafka:${CONFIG.region}:${CONFIG.account}:topic/${mskClusterName}/*`,
        `arn:aws:kafka:${CONFIG.region}:${CONFIG.account}:group/${mskClusterName}/*`,
      ],
    }));

    const s3TablesBase = `arn:aws:s3tables:${CONFIG.region}:${CONFIG.account}:bucket/${CONFIG.s3TablesBucketName}`;
    role.addToPolicy(new iam.PolicyStatement({
      actions: [
        's3tables:GetTable', 's3tables:GetTableBucket', 's3tables:GetTableMetadataLocation',
        's3tables:PutTableData', 's3tables:UpdateTableMetadataLocation',
        's3tables:GetTableMaintenanceConfiguration', 's3tables:GetTableMaintenanceJobStatus',
      ],
      resources: [s3TablesBase, `${s3TablesBase}/*`],
    }));

    role.addToPolicy(new iam.PolicyStatement({
      actions: [
        'glue:GetDatabase', 'glue:GetTable', 'glue:GetTableVersion', 'glue:GetTableVersions', 'glue:UpdateTable',
        'lakeformation:GetDataAccess',
        's3:GetObject', 's3:PutObject', 's3:DeleteObject', 's3:ListBucket', 's3:GetBucketLocation',
        'ec2:CreateNetworkInterface', 'ec2:DeleteNetworkInterface', 'ec2:DescribeNetworkInterfaces',
        'ec2:DescribeSecurityGroups', 'ec2:DescribeSubnets', 'ec2:DescribeVpcs',
      ],
      resources: ['*'],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['lambda:InvokeFunction', 'lambda:GetFunctionConfiguration'],
      resources: [props.lambdaArn],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['logs:CreateLogGroup', 'logs:CreateLogStream', 'logs:PutLogEvents'],
      resources: [`arn:aws:logs:${CONFIG.region}:${CONFIG.account}:log-group:/aws/kinesisfirehose/*`],
    }));

    this.roleArn = role.roleArn;
    new cdk.CfnOutput(this, 'RoleArn', { value: role.roleArn });
  }
}
