// Shared config — exact values from deployed environment
export const CONFIG = {
  account: '063337766236',
  region: 'us-east-1',

  // VPC / Networking
  vpcId: 'vpc-06d31004a0ac2b8de',
  subnetIds: ['subnet-0215dd02b9e3edbde', 'subnet-0ad591a7a23c94b6c'],
  auroraSecurityGroupId: 'sg-06df333226cad343e', // Aurora cluster SG — MSK SG gets ingress from this
  // Note: MSK SG is now created by the MskClusterStack (no longer a static config value)

  // Aurora
  auroraEndpoint: 'aurora-cdc-rds-auroracluster-l1vzx1j2gxdb.cluster-crkeeemtkh5z.us-east-1.rds.amazonaws.com',
  auroraPort: '5432',
  auroraDbName: 'cdcdemo',
  auroraUser: 'dbadmin',
  auroraSecretArn: 'arn:aws:secretsmanager:us-east-1:063337766236:secret:aurora-cdc-credentials-25ImGm',

  // MSK
  mskClusterName: 'aurora-cdc-cluster',
  mskKafkaVersion: '3.7.x',
  mskInstanceType: 'kafka.m5.large',
  mskBrokerCount: 2,
  mskEbsVolumeSize: 100,
  mskBootstrapServers: 'b-2.auroracdccluster.5yymx6.c18.kafka.us-east-1.amazonaws.com:9092,b-1.auroracdccluster.5yymx6.c18.kafka.us-east-1.amazonaws.com:9092',

  // Debezium / MSK Connect
  // NOTE: Plugin and worker config must be created via CLI before deploying the connector stack.
  //   1. Run cloudformation/build-debezium-plugin.sh to build and upload plugin ZIP
  //   2. Create custom plugin: aws kafkaconnect create-custom-plugin ...
  //   3. Create worker config: aws kafkaconnect create-worker-configuration ...
  //   4. Update these ARNs below
  debeziumPluginArn: 'arn:aws:kafkaconnect:us-east-1:063337766236:custom-plugin/debezium-postgres-connector/5fbf9bb4-d01b-40c6-835b-18ad7e5520af-4',
  debeziumWorkerConfigArn: 'arn:aws:kafkaconnect:us-east-1:063337766236:worker-configuration/debezium-worker-config/60c6a306-91aa-4d93-8bd2-275bc9a3d49a-4',
  debeziumPluginBucket: 'msk-connect-plugins-063337766236-us-east-1',
  debeziumTopicPrefix: 'aurora.cdc',
  debeziumTables: 'public.orders,public.products',

  // S3 Tables
  s3TablesBucketName: 'aurora-cdc-table-bucket-063337766236-us-east-1',
  s3TablesNamespace: 'aurora_cdc',
  tables: ['orders', 'products'] as const,
  tableKeys: { orders: 'order_id', products: 'product_id' } as Record<string, string>,

  // Firehose
  firehoseBackupBucket: 'firehose-backup-063337766236-us-east-1',
};
