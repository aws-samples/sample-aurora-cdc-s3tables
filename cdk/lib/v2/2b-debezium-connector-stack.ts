import * as cdk from 'aws-cdk-lib';
import * as kafkaconnect from 'aws-cdk-lib/aws-kafkaconnect';
import { Construct } from 'constructs';
import { CONFIG } from './config';

interface Props extends cdk.StackProps {
  mskClusterArn: string;
  mskBootstrapServers: string;
  mskSecurityGroupId: string;
  serviceRoleArn: string;
  customPluginArn: string;
  workerConfigArn: string;
}

export class DebeziumConnectorStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: Props) {
    super(scope, id, props);

    const connector = new kafkaconnect.CfnConnector(this, 'Connector', {
      connectorName: 'aurora-postgres-debezium-connector',
      connectorDescription: 'Debezium CDC connector for Aurora PostgreSQL',
      kafkaCluster: {
        apacheKafkaCluster: {
          bootstrapServers: props.mskBootstrapServers,
          vpc: {
            subnets: CONFIG.subnetIds,
            securityGroups: [props.mskSecurityGroupId],
          },
        },
      },
      kafkaClusterClientAuthentication: { authenticationType: 'NONE' },
      kafkaClusterEncryptionInTransit: { encryptionType: 'PLAINTEXT' },
      kafkaConnectVersion: '2.7.1',
      plugins: [{
        customPlugin: {
          customPluginArn: props.customPluginArn,
          revision: 1,
        },
      }],
      serviceExecutionRoleArn: props.serviceRoleArn,
      capacity: {
        provisionedCapacity: { mcuCount: 2, workerCount: 2 },
      },
      workerConfiguration: {
        workerConfigurationArn: props.workerConfigArn,
        revision: 1,
      },
      connectorConfiguration: {
        'connector.class': 'io.debezium.connector.postgresql.PostgresConnector',
        'tasks.max': '1',
        'database.hostname': CONFIG.auroraEndpoint,
        'database.port': CONFIG.auroraPort,
        'database.user': CONFIG.auroraUser,
        'database.dbname': CONFIG.auroraDbName,
        'database.server.name': 'aurora_cdc',
        'plugin.name': 'pgoutput',
        'slot.name': 'debezium_slot',
        'publication.name': 'dbz_publication',
        'table.include.list': CONFIG.debeziumTables,
        'topic.prefix': CONFIG.debeziumTopicPrefix,
        'schema.history.internal.kafka.topic': 'schema-changes.aurora',
        'schema.history.internal.kafka.bootstrap.servers': props.mskBootstrapServers,
        'decimal.handling.mode': 'string',
        'time.precision.mode': 'adaptive_time_microseconds',
        'tombstones.on.delete': 'false',
        'snapshot.mode': 'initial',
        'publication.autocreate.mode': 'filtered',
        'transforms': 'Reroute',
        'transforms.Reroute.type': 'io.debezium.transforms.ByLogicalTableRouter',
        'transforms.Reroute.topic.regex': 'aurora\\.cdc\\.public\\.(.*)',
        'transforms.Reroute.topic.replacement': 'aurora.cdc.all-tables',
      },
      logDelivery: {
        workerLogDelivery: {
          cloudWatchLogs: {
            enabled: true,
            logGroup: '/aws/msk-connect/aurora-cdc-debezium',
          },
        },
      },
    });
  }
}
