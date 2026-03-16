import * as cdk from 'aws-cdk-lib';
import * as cr from 'aws-cdk-lib/custom-resources';
import { Construct } from 'constructs';
import { CONFIG } from './config';

interface Props extends cdk.StackProps {
  firehoseRoleArn: string;
  mskClusterArn: string;
}

export class LakeFormationStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id, props);

    const catalogId = `${CONFIG.account}:s3tablescatalog/${CONFIG.s3TablesBucketName}`;

    new cr.AwsCustomResource(this, 'DbPermissions', {
      onCreate: {
        service: 'LakeFormation',
        action: 'grantPermissions',
        parameters: {
          Principal: { DataLakePrincipalIdentifier: props.firehoseRoleArn },
          Resource: { Database: { CatalogId: catalogId, Name: CONFIG.s3TablesNamespace } },
          Permissions: ['ALL'],
        },
        physicalResourceId: cr.PhysicalResourceId.of('lf-db-perm'),
      },
      onDelete: {
        service: 'LakeFormation',
        action: 'revokePermissions',
        parameters: {
          Principal: { DataLakePrincipalIdentifier: props.firehoseRoleArn },
          Resource: { Database: { CatalogId: catalogId, Name: CONFIG.s3TablesNamespace } },
          Permissions: ['ALL'],
        },
      },
      policy: cr.AwsCustomResourcePolicy.fromSdkCalls({ resources: cr.AwsCustomResourcePolicy.ANY_RESOURCE }),
    });

    new cr.AwsCustomResource(this, 'TablePermissions', {
      onCreate: {
        service: 'LakeFormation',
        action: 'grantPermissions',
        parameters: {
          Principal: { DataLakePrincipalIdentifier: props.firehoseRoleArn },
          Resource: { Table: { CatalogId: catalogId, DatabaseName: CONFIG.s3TablesNamespace, TableWildcard: {} } },
          Permissions: ['ALL'],
        },
        physicalResourceId: cr.PhysicalResourceId.of('lf-table-perm'),
      },
      onDelete: {
        service: 'LakeFormation',
        action: 'revokePermissions',
        parameters: {
          Principal: { DataLakePrincipalIdentifier: props.firehoseRoleArn },
          Resource: { Table: { CatalogId: catalogId, DatabaseName: CONFIG.s3TablesNamespace, TableWildcard: {} } },
          Permissions: ['ALL'],
        },
      },
      policy: cr.AwsCustomResourcePolicy.fromSdkCalls({ resources: cr.AwsCustomResourcePolicy.ANY_RESOURCE }),
    });

    new cr.AwsCustomResource(this, 'MskClusterPolicy', {
      onCreate: {
        service: 'Kafka',
        action: 'putClusterPolicy',
        parameters: {
          ClusterArn: props.mskClusterArn,
          Policy: JSON.stringify({
            Version: '2012-10-17',
            Statement: [{
              Effect: 'Allow',
              Principal: { Service: 'firehose.amazonaws.com' },
              Action: ['kafka:CreateVpcConnection', 'kafka:GetBootstrapBrokers', 'kafka:DescribeClusterV2'],
              Resource: props.mskClusterArn,
            }],
          }),
        },
        physicalResourceId: cr.PhysicalResourceId.of('msk-cluster-policy'),
      },
      policy: cr.AwsCustomResourcePolicy.fromSdkCalls({ resources: cr.AwsCustomResourcePolicy.ANY_RESOURCE }),
    });
  }
}
