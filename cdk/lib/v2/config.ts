// Shared config - replace placeholder values with your environment details before deploying.
// See README.md for configuration instructions.
export const CONFIG = {
  account: '<your-account-id>',
  region: '<your-region>',

  // VPC / Networking
  vpcId: '<your-vpc-id>',
  subnetIds: ['<subnet-1>', '<subnet-2>'],
  auroraSecurityGroupId: '<aurora-security-group-id>',

  // Aurora
  auroraEndpoint: '<aurora-cluster-endpoint>',
  auroraPort: '5432',
  auroraDbName: '<database-name>',
  auroraUser: '<db-user>',
  auroraSecretArn: '<secrets-manager-arn>',

  // MSK
  mskClusterName: 'aurora-cdc-cluster',
  mskKafkaVersion: '3.7.x',
  mskInstanceType: 'kafka.m5.large',
  mskBrokerCount: 2,
  mskEbsVolumeSize: 100,
  mskBootstrapServers: '<bootstrap-servers>', // Available after MSK cluster is created

  // Debezium / MSK Connect
  // NOTE: Plugin and worker config must be created via CLI before deploying the connector stack.
  //   1. Build and upload plugin ZIP (see README Step 2)
  //   2. Create custom plugin: aws kafkaconnect create-custom-plugin ...
  //   3. Create worker config: aws kafkaconnect create-worker-configuration ...
  //   4. Update these ARNs below
  debeziumPluginArn: '<custom-plugin-arn>',
  debeziumWorkerConfigArn: '<worker-configuration-arn>',
  debeziumPluginBucket: '<your-plugin-bucket-name>',
  debeziumTopicPrefix: 'aurora.cdc',
  debeziumTables: 'public.orders,public.products',

  // S3 Tables
  s3TablesBucketName: '<your-table-bucket-name>',
  s3TablesNamespace: 'aurora_cdc',
  tables: ['orders', 'products'] as const,
  tableKeys: { orders: 'order_id', products: 'product_id' } as Record<string, string>,

  // Firehose
  firehoseBackupBucket: '<your-backup-bucket-name>',
};
