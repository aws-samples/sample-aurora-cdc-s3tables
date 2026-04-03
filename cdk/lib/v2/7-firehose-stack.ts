import * as cdk from 'aws-cdk-lib';
import * as firehose from 'aws-cdk-lib/aws-kinesisfirehose';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { CONFIG } from './config';

interface Props extends cdk.StackProps {
  roleArn: string;
  mskClusterArn: string;
  lambdaArn: string;
}

export class FirehoseStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id, props);

    const logGroup = new logs.LogGroup(this, 'LogGroup', {
      logGroupName: '/aws/kinesisfirehose/msk-to-s3tables',
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });
    const logStream = new logs.LogStream(this, 'LogStream', { logGroup, logStreamName: 'IcebergDelivery' });

    new firehose.CfnDeliveryStream(this, 'Firehose', {
      deliveryStreamName: 'msk-to-s3tables-firehose',
      deliveryStreamType: 'MSKAsSource',
      mskSourceConfiguration: {
        mskClusterArn: props.mskClusterArn,
        topicName: 'aurora.cdc.all-tables',
        authenticationConfiguration: {
          roleArn: props.roleArn,
          connectivity: 'PUBLIC',
        },
      },
      icebergDestinationConfiguration: {
        roleArn: props.roleArn,
        catalogConfiguration: {
          catalogArn: `arn:aws:glue:${CONFIG.region}:${CONFIG.account}:catalog/s3tablescatalog/${CONFIG.s3TablesBucketName}`,
        },
        destinationTableConfigurationList: CONFIG.tables.map(t => ({
          destinationDatabaseName: CONFIG.s3TablesNamespace,
          destinationTableName: t,
          s3ErrorOutputPrefix: `errors/${t}/`,
          uniqueKeys: [CONFIG.tableKeys[t]],
        })),
        processingConfiguration: {
          enabled: true,
          processors: [{
            type: 'Lambda',
            parameters: [
              { parameterName: 'LambdaArn', parameterValue: props.lambdaArn },
              { parameterName: 'BufferSizeInMBs', parameterValue: '1' },
              { parameterName: 'BufferIntervalInSeconds', parameterValue: '60' },
            ],
          }],
        },
        bufferingHints: { intervalInSeconds: 60, sizeInMBs: 1 },
        cloudWatchLoggingOptions: {
          enabled: true,
          logGroupName: logGroup.logGroupName,
          logStreamName: logStream.logStreamName,
        },
        s3Configuration: {
          bucketArn: `arn:aws:s3:::${CONFIG.firehoseBackupBucket}`,
          roleArn: props.roleArn,
          prefix: 'backup/',
          errorOutputPrefix: 'errors/',
          bufferingHints: { intervalInSeconds: 300, sizeInMBs: 5 },
          compressionFormat: 'GZIP',
        },
      },
    });
  }
}
