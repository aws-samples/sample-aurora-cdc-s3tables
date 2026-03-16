import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as msk from 'aws-cdk-lib/aws-msk';
import * as logs from 'aws-cdk-lib/aws-logs';
import { Construct } from 'constructs';
import { CONFIG } from './config';

/**
 * MSK Cluster Stack — deploys the exact cluster config from the working pipeline:
 *
 * Key config (all applied, previously done via 3 separate CLI updates):
 *   1. IAM auth enabled          — required for Firehose to consume from MSK
 *   2. Unauthenticated enabled   — required for Debezium connector (PLAINTEXT)
 *   3. VPC connectivity with IAM — required for Firehose PRIVATE connectivity (PrivateLink)
 *   4. auto.create.topics=true   — required for Debezium to auto-create topics
 *
 * Security groups created here (previously from cloudformation/msk-cluster.yaml):
 *   - MSK SG with ingress from Aurora SG (9092, 9094, 2181)
 *   - Self-referencing rule on MSK SG (9092) for MSK Connect workers
 *   - Reverse rule: Aurora SG ingress from MSK SG on 5432 (for Debezium → Aurora)
 *
 * Integration points:
 *   - clusterArn  → used by Debezium connector, Firehose, LakeFormation, FirehoseRole stacks
 *   - securityGroupId → used by Debezium connector stack (workers run in this SG)
 */
export class MskClusterStack extends cdk.Stack {
  public readonly clusterArn: string;
  public readonly securityGroupId: string;

  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const vpc = ec2.Vpc.fromLookup(this, 'Vpc', { vpcId: CONFIG.vpcId });

    // --- Security Groups ---

    const mskSg = new ec2.SecurityGroup(this, 'MskSg', {
      vpc,
      securityGroupName: 'msk-aurora-cdc-sg',
      description: 'Security group for MSK cluster',
      allowAllOutbound: true,
    });

    const auroraSg = ec2.SecurityGroup.fromSecurityGroupId(this, 'AuroraSg', CONFIG.auroraSecurityGroupId);

    // Aurora SG → MSK: Kafka plaintext, TLS, Zookeeper
    mskSg.addIngressRule(auroraSg, ec2.Port.tcp(9092), 'Kafka plaintext from Aurora SG');
    mskSg.addIngressRule(auroraSg, ec2.Port.tcp(9094), 'Kafka TLS from Aurora SG');
    mskSg.addIngressRule(auroraSg, ec2.Port.tcp(9098), 'Kafka IAM from Aurora SG');
    mskSg.addIngressRule(auroraSg, ec2.Port.tcp(2181), 'Zookeeper from Aurora SG');

    // Self-referencing: MSK Connect workers communicate with brokers
    mskSg.addIngressRule(mskSg, ec2.Port.tcp(9092), 'Self-ref for MSK Connect workers (plaintext)');
    mskSg.addIngressRule(mskSg, ec2.Port.tcp(9098), 'Self-ref for MSK Connect workers (IAM)');

    // Reverse: MSK Connect workers (in MSK SG) → Aurora on 5432 (for Debezium CDC reads)
    auroraSg.addIngressRule(mskSg, ec2.Port.tcp(5432), 'Allow MSK Connect workers to access Aurora');

    // --- Broker Logs ---

    const logGroup = new logs.LogGroup(this, 'BrokerLogs', {
      logGroupName: `/aws/msk/${CONFIG.mskClusterName}`,
      retention: logs.RetentionDays.ONE_WEEK,
      removalPolicy: cdk.RemovalPolicy.DESTROY,
    });

    // --- Custom Configuration ---
    // auto.create.topics.enable=true: Debezium needs this to create CDC topics on first connect.
    // Without it, connector fails with "topic not found" errors.

    const config = new msk.CfnConfiguration(this, 'Config', {
      name: `${CONFIG.mskClusterName}-config`,
      kafkaVersionsList: [CONFIG.mskKafkaVersion],
      serverProperties: 'auto.create.topics.enable=true\n',
    });

    // --- MSK Cluster ---

    const cluster = new msk.CfnCluster(this, 'Cluster', {
      clusterName: CONFIG.mskClusterName,
      kafkaVersion: CONFIG.mskKafkaVersion,
      numberOfBrokerNodes: CONFIG.mskBrokerCount,
      configurationInfo: {
        arn: config.attrArn,
        revision: 1,
      },
      brokerNodeGroupInfo: {
        instanceType: CONFIG.mskInstanceType,
        clientSubnets: CONFIG.subnetIds,
        securityGroups: [mskSg.securityGroupId],
        storageInfo: { ebsStorageInfo: { volumeSize: CONFIG.mskEbsVolumeSize } },
        // VPC connectivity with IAM auth — creates the multi-VPC endpoint that
        // Firehose uses via PrivateLink (PRIVATE connectivity mode).
        // This was CLI "Update 2" — each MSK update required a rolling broker restart (~20-30 min).
        connectivityInfo: {
          vpcConnectivity: {
            clientAuthentication: { sasl: { iam: { enabled: true } } },
          },
        },
      },
      // Client authentication:
      //   IAM enabled   → Firehose authenticates via IAM (required for MSK source)
      //   Unauthenticated enabled → Debezium connector uses PLAINTEXT (no IAM)
      // Both were enabled via CLI "Update 1" — originally cluster had only unauthenticated.
      clientAuthentication: {
        sasl: { iam: { enabled: true } },
        unauthenticated: { enabled: true },
      },
      encryptionInfo: {
        encryptionInTransit: {
          clientBroker: 'TLS_PLAINTEXT', // Supports both TLS (IAM) and PLAINTEXT (Debezium)
          inCluster: true,
        },
      },
      loggingInfo: {
        brokerLogs: {
          cloudWatchLogs: { enabled: true, logGroup: logGroup.logGroupName },
        },
      },
      tags: { Project: 'aurora-cdc-s3tables', Purpose: 'debezium-cdc' },
    });

    this.clusterArn = cluster.attrArn;
    this.securityGroupId = mskSg.securityGroupId;

    new cdk.CfnOutput(this, 'ClusterArn', { value: cluster.attrArn });
    new cdk.CfnOutput(this, 'SecurityGroupId', { value: mskSg.securityGroupId });
  }
}
