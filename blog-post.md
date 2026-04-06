# Real-time CDC from Amazon Aurora PostgreSQL to Apache Iceberg tables in Amazon S3 Tables

Organizations running transactional workloads on [Amazon Aurora PostgreSQL-Compatible Edition](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.AuroraPostgreSQL.html) (Aurora PostgreSQL) often need to make operational data available for analytics without impacting production performance. This becomes especially challenging when data is distributed across multiple Aurora clusters, making it difficult to join datasets and build cross-domain analytics workflows.

Lakehouse architectures built on [Apache Iceberg](https://iceberg.apache.org/) address these challenges with a unified data layer that supports ACID transactions, schema evolution, and time travel. [Amazon S3 Tables](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html), a capability of [Amazon Simple Storage Service](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html) (Amazon S3), offers purpose-built storage for Apache Iceberg tables with automatic snapshot management and compaction, making it a foundation for a governed lakehouse architecture.

However, you still need a reliable way to continuously ingest operational database changes into the lakehouse while preserving transactional performance.

In this post, we show you how to build a near real-time change data capture (CDC) pipeline that streams data from Aurora PostgreSQL into Apache Iceberg tables in Amazon S3 Tables using [Debezium](https://debezium.io/), [Amazon MSK Connect](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect.html), [AWS Lambda](https://docs.aws.amazon.com/lambda/latest/dg/welcome.html), and [Amazon Data Firehose](https://docs.aws.amazon.com/firehose/latest/dev/what-is-this-service.html). We deploy the infrastructure using the [AWS Cloud Development Kit](https://docs.aws.amazon.com/cdk/v2/guide/home.html) (AWS CDK). By the end of this walkthrough, you have a working pipeline that captures inserts, updates, and deletes from Aurora and applies them as row-level Iceberg operations in S3 Tables.

## Solution overview

The following diagram shows the architecture of the CDC pipeline.

![Architecture Diagram](screenshots/aurora-cdc-s3tables-architecture.png)

*Figure 1. CDC pipeline architecture from Aurora PostgreSQL to Amazon S3 Tables.*

The pipeline uses six components:

1. **Aurora PostgreSQL** as the source database with logical replication enabled
2. **Debezium on MSK Connect** to capture row-level changes from the PostgreSQL write-ahead log (WAL)
3. **Amazon MSK** as the streaming backbone
4. **Lambda** to transform Debezium CDC events and route them to the correct destination table
5. **Firehose** to deliver records from MSK to Apache Iceberg tables
6. **S3 Tables** as the managed Iceberg destination with automatic compaction and snapshot management

A key design decision is the single-topic routing pattern. Firehose supports only one MSK topic per delivery stream. Without routing, each source table would need its own Firehose stream and [VPC connection](https://docs.aws.amazon.com/msk/latest/developerguide/aws-access-mult-vpc.html). Instead, you use a Debezium [Single Message Transform](https://debezium.io/documentation/reference/stable/transformations/topic-routing.html) (SMT) to route changes from the monitored tables into a single MSK topic, and the Lambda function directs each record to the correct Iceberg table. This approach uses one Firehose stream for multiple tables, reducing cost and operational complexity.

The data flows through five stages:

1. **Aurora PostgreSQL to Debezium.** Debezium runs on MSK Connect as worker JVMs with [Elastic Network Interfaces](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-eni.html) (ENIs) in your VPC. The workers connect to Aurora PostgreSQL on port 5432 and use PostgreSQL's native [logical replication](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html) protocol (via the `pgoutput` plugin) to stream changes from the WAL. Aurora pushes WAL changes to Debezium over a persistent TCP connection, with minimal impact on query performance because Debezium reads from the WAL rather than querying the tables directly.

2. **Debezium to Amazon MSK.** The Debezium worker serializes each change as a JSON-encoded Kafka message and publishes it to the MSK cluster on port 9092. The `ByLogicalTableRouter` SMT reroutes events from multiple tables (for example, `aurora.cdc.public.orders` and `aurora.cdc.public.products`) into a single topic (`aurora.cdc.all-tables`). Each message retains the original source table name in the Debezium envelope.

3. **Amazon MSK to Firehose.** Firehose connects to the MSK cluster using [IAM access control](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html) over an [AWS PrivateLink](https://docs.aws.amazon.com/vpc/latest/privatelink/what-is-privatelink.html) connection. Traffic stays within the AWS network. Firehose continuously polls the `aurora.cdc.all-tables` topic for new messages.

4. **Firehose to Lambda.** For each batch of records, Firehose invokes the Lambda function synchronously. The function decodes the Kafka message, flattens the Debezium envelope, and sets `otfMetadata` routing with the destination table name and operation type (`insert`, `update`, or `delete`).

5. **Firehose to S3 Tables.** Firehose reads the `otfMetadata` from each transformed record and routes it to the correct Iceberg table. Using the configured unique keys (for example, `order_id` for orders), Firehose performs the appropriate row-level operation. Firehose buffers records based on [buffering hints](https://docs.aws.amazon.com/firehose/latest/APIReference/API_BufferingHints.html) (these are treated as hints, and Firehose may choose different values when optimal) and writes them as Parquet data files. S3 Tables handles Iceberg snapshot management and [compaction](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-maintenance-compaction.html) automatically.

After data lands in S3 Tables, you can use [AWS Lake Formation](https://docs.aws.amazon.com/lake-formation/latest/dg/what-is-lake-formation.html) for access control and query the Iceberg tables with [Amazon Athena](https://docs.aws.amazon.com/athena/latest/ug/what-is.html), [Amazon Redshift](https://docs.aws.amazon.com/redshift/latest/mgmt/welcome.html), or [Amazon SageMaker Unified Studio](https://docs.aws.amazon.com/sagemaker-unified-studio/latest/userguide/what-is-sagemaker-unified-studio.html).

### Debezium event transformation

Debezium produces CDC events in an [envelope structure](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-events) containing both the previous and current state of a row, along with metadata about the source database, table, and operation type. However, Firehose expects records in a flattened JSON format with routing metadata that indicates the target table and operation type.

The Lambda function bridges this gap by performing three operations on each record:

1. **Decode.** Extracts the base64-encoded Kafka message from the `kafkaRecordValue` field. When Firehose uses Amazon MSK as a source, incoming records use the `kafkaRecordValue` field rather than the `data` field used with [Amazon Kinesis Data Streams](https://docs.aws.amazon.com/streams/latest/dev/introduction.html) or Direct PUT sources.

2. **Flatten and extract.** Pulls the row data from the Debezium envelope. For inserts and updates, the function uses the `after` field (the row after the change). For deletes, it uses the `before` field, because the `after` field is null when a row is removed.

3. **Route.** Sets the [`otfMetadata`](https://docs.aws.amazon.com/firehose/latest/dev/apache-iceberg-format-input-record-different.html) block with `destinationTableName` (extracted from the Debezium `source.table` field) and `operation` (mapped from Debezium's single-character codes to Firehose's operation types).

The following table shows how Debezium operation codes map to Firehose Iceberg operations:

| Debezium code | Meaning | Firehose operation |
|---------------|---------|-------------------|
| `c` | Row created (insert) | `insert` |
| `u` | Row updated | `update` |
| `d` | Row deleted | `delete` |
| `r` | Snapshot read (initial load) | `insert` |

For example, the function transforms this Debezium envelope:

```json
{
  "op": "c",
  "before": null,
  "after": {"order_id": 1, "customer_id": 1, "total_amount": 299.99},
  "source": {"table": "orders", "db": "cdcdemo"}
}
```

Into a response record with routing metadata:

```json
{
  "recordId": "<original-record-id>",
  "result": "Ok",
  "kafkaRecordValue": "<base64-encoded flattened row JSON>",
  "metadata": {
    "otfMetadata": {
      "destinationDatabaseName": "aurora_cdc",
      "destinationTableName": "orders",
      "operation": "insert"
    }
  }
}
```

The `kafkaRecordValue` contains the base64-encoded flattened row data (for example, `{"order_id": 1, "customer_id": 1, "total_amount": 299.99}`), and the `otfMetadata` block tells Firehose which table to write to and which operation to perform.

This routing metadata is what enables a single Firehose stream to write to multiple destination tables. For more information, see [Route incoming records to different Iceberg tables](https://docs.aws.amazon.com/firehose/latest/dev/apache-iceberg-format-input-record-different.html).

## Prerequisites

Before you begin, make sure you have the following:

- An [AWS account](https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/) with permissions to create the resources described in this post
- An existing [Amazon Virtual Private Cloud](https://docs.aws.amazon.com/vpc/latest/userguide/what-is-amazon-vpc.html) (Amazon VPC) with at least two subnets in different Availability Zones
- An Aurora PostgreSQL cluster in the same VPC with [logical replication enabled](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html) (`rds.logical_replication = 1`)
- Aurora database credentials stored in [AWS Secrets Manager](https://docs.aws.amazon.com/secretsmanager/latest/userguide/intro.html). Note the secret ARN for the CDK configuration.
- [AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting_started.html) v2 installed (`npm install -g aws-cdk`)
- [Node.js](https://nodejs.org/) 18+ and npm
- [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html) v2 installed and configured with appropriate credentials
- An [Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html) general purpose bucket for the Debezium plugin upload and Firehose failed record backup
- [S3 Tables integration with AWS analytics services](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-aws.html) enabled in your Region (one-time setup)

## Walkthrough

The following steps walk you through building the CDC pipeline end to end.

### Step 1: Enable CDC in Aurora PostgreSQL

PostgreSQL supports change data capture through its [logical replication](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html) framework, which allows database changes to be streamed from the write-ahead log (WAL). Debezium uses this mechanism to continuously read row-level changes and publish them to Kafka topics.

To enable logical replication in Aurora PostgreSQL, configure a custom [DB cluster parameter group](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_WorkingWithDBClusterParamGroups.html):

1. Create a custom parameter group and set the following parameter:

   ```
   rds.logical_replication = 1
   ```

2. Apply the parameter group to your Aurora cluster and reboot the cluster for the change to take effect.

3. Connect to your Aurora PostgreSQL cluster and create the source tables:

```sql
CREATE TABLE public.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER,
    order_date VARCHAR(50),
    total_amount DECIMAL(12,2),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE public.products (
    product_id SERIAL PRIMARY KEY,
    product_name VARCHAR(255),
    category VARCHAR(100),
    price DECIMAL(10,2),
    stock_quantity INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

4. Create a [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) that defines which tables are included in the change stream. Debezium automatically creates the [logical replication slot](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html#LOGICALDECODING-REPLICATION-SLOTS) when the connector starts for the first time, so you do not need to create one manually.

```sql
CREATE PUBLICATION dbz_publication FOR TABLE public.orders, public.products;
```

5. Verify the publication was created:

```sql
SELECT * FROM pg_publication WHERE pubname = 'dbz_publication';
```

You should see one row returned, confirming the publication is active.

> **Important:** When the Debezium connector starts (Step 6), it creates a replication slot named `debezium_slot`. This slot retains WAL segments until consumed. If the connector is stopped for an extended period, WAL segments can accumulate and increase storage usage on the Aurora cluster. Monitor the `ReplicationSlotDiskUsage` [Amazon CloudWatch](https://docs.aws.amazon.com/AmazonCloudWatch/latest/monitoring/WhatIsCloudWatch.html) metric for your Aurora cluster.

### Step 2: Build and register the Debezium plugin

[MSK Connect](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect.html) runs connectors using [custom plugins](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-plugins.html) that you upload to Amazon S3. In this step, you download the Debezium PostgreSQL connector, package it as a ZIP file, upload it to S3, and register it with MSK Connect.

First, create an S3 bucket for the plugin, or use an existing metadata management bucket:

```bash
aws s3 mb s3://<your-plugin-bucket> --region <your-region>
```

Download and package the Debezium connector:

```bash
DEBEZIUM_VERSION=2.7.3.Final
curl -LO "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz"

mkdir -p debezium-plugin
tar -xzf debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz -C debezium-plugin/
cd debezium-plugin && zip -r ../debezium-postgres-connector.zip . && cd ..

aws s3 cp debezium-postgres-connector.zip s3://<your-plugin-bucket>/plugins/
```

Register the plugin with MSK Connect:

```bash
aws kafkaconnect create-custom-plugin \
  --custom-plugin-name debezium-postgres-connector \
  --content-type ZIP \
  --location "s3Location={bucketArn=arn:aws:s3:::<your-plugin-bucket>,fileKey=plugins/debezium-postgres-connector.zip}"
```

Create a [worker configuration](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect-workers.html) that tells MSK Connect to serialize Kafka messages as JSON without schemas:

```bash
aws kafkaconnect create-worker-configuration \
  --name debezium-worker-config \
  --properties-file-content "$(echo -n 'key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false' | base64)"
```

Note the `customPluginArn` and `workerConfigurationArn` from the output - you need these for the CDK configuration in the next step.

> **Note:** The custom plugin and worker configuration are created via the AWS CLI because the Debezium connector JARs must be downloaded from the [Debezium project](https://debezium.io/releases/) and packaged manually. The remaining infrastructure is deployed using the AWS CDK in the following steps.

### Step 3: Configure the CDK project

Clone the sample repository and install dependencies:

```bash
git clone https://github.com/aws-samples/sample-aurora-cdc-s3tables.git
cd sample-aurora-cdc-s3tables/cdk
npm install
```

Open `cdk/lib/v2/config.ts` and update the configuration values to match your environment:

```typescript
export const CONFIG = {
  account: '<your-account-id>',
  region: '<your-region>',

  // VPC - must match your Aurora cluster's VPC
  vpcId: '<your-vpc-id>',
  subnetIds: ['<subnet-1>', '<subnet-2>'],
  auroraSecurityGroupId: '<aurora-security-group-id>',

  // Aurora connection details
  auroraEndpoint: '<aurora-cluster-endpoint>',
  auroraPort: '5432',
  auroraDbName: '<database-name>',
  auroraUser: '<db-user>',
  auroraSecretArn: '<secrets-manager-arn>',

  // Debezium - use the ARNs from Step 2
  debeziumPluginArn: '<customPluginArn-from-step-2>',
  debeziumWorkerConfigArn: '<workerConfigurationArn-from-step-2>',
  debeziumPluginBucket: '<your-plugin-bucket-name>',
  debeziumTopicPrefix: 'aurora.cdc',
  debeziumTables: 'public.orders,public.products',

  // S3 Tables - the table bucket name must be globally unique
  s3TablesBucketName: '<your-table-bucket-name>',
  s3TablesNamespace: 'aurora_cdc',
  tables: ['orders', 'products'],
  tableKeys: { orders: 'order_id', products: 'product_id' },

  // Firehose - general purpose S3 bucket for failed record backup
  firehoseBackupBucket: '<your-backup-bucket-name>',
};
```

Key configuration notes:

- **`auroraSecurityGroupId`.** The [security group](https://docs.aws.amazon.com/vpc/latest/userguide/vpc-security-groups.html) attached to your Aurora cluster. The CDK creates an MSK security group with ingress rules allowing traffic from this security group, and a reverse rule allowing MSK Connect workers to reach Aurora on port 5432.
- **`tableKeys`.** The primary key column for each table. Firehose uses these to match incoming records against existing rows for [update and delete operations](https://docs.aws.amazon.com/firehose/latest/dev/apache-iceberg-destination.html) in the Iceberg tables.
- **`s3TablesBucketName`.** The name for your S3 table bucket. Table bucket names must be [unique for your account in the chosen Region](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-buckets-naming.html).

### Step 4: Deploy the CDK stacks

Deploy all six stacks with a single command. The CDK resolves the dependency order automatically:

```bash
npx cdk --app "npx ts-node bin/app-v2.ts" deploy --all
```

When prompted, review the [IAM](https://docs.aws.amazon.com/IAM/latest/UserGuide/introduction.html) changes and confirm the deployment. The CDK deploys the following stacks:

| Stack | What it creates |
|-------|----------------|
| `CdcMskCluster` | Amazon MSK cluster (2x kafka.m5.large brokers) with dual authentication ([IAM](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html) for Firehose, unauthenticated for Debezium), custom configuration with `auto.create.topics.enable=true`, security groups with ingress rules for Aurora and MSK Connect workers |
| `CdcMskConnectIam` | MSK Connect service execution role with permissions for Kafka cluster operations, VPC networking, S3 plugin access, and [AWS Secrets Manager](https://docs.aws.amazon.com/secretsmanager/latest/userguide/intro.html); [Amazon CloudWatch Logs](https://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/WhatIsCloudWatchLogs.html) group for connector logs |
| `CdcS3Tables` | S3 table bucket, `aurora_cdc` namespace, two Iceberg tables (`orders`, `products`) with column schemas |
| `CdcLambdaTransform` | Lambda function for CDC event transformation and multi-table routing |
| `CdcFirehoseRole` | Firehose IAM role with permissions for Amazon MSK, S3 Tables, [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html), [AWS Lake Formation](https://docs.aws.amazon.com/lake-formation/latest/dg/what-is-lake-formation.html), VPC networking, and Lambda invocation |
| `CdcFirehose` | Firehose delivery stream with MSK as source (private connectivity via AWS PrivateLink), Lambda processing, Apache Iceberg Tables as destination with two table configurations, and S3 backup bucket for failed records |

The MSK cluster takes approximately 25 minutes to create. The Debezium connector takes approximately 5 minutes after the cluster is ready. You can monitor the deployment progress in the [AWS CloudFormation](https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/Welcome.html) console.

After the deployment completes, you can verify the resources in the AWS console. The S3 table bucket shows the two Iceberg tables in the `aurora_cdc` namespace.

![S3 Table Bucket](screenshots/aurora-cdc-table-bucket.png)

*Figure 2. S3 table bucket showing the orders and products Iceberg tables in the aurora_cdc namespace.*

The Firehose delivery stream shows the MSK source, Lambda transformation, and Apache Iceberg Tables destination.

![Firehose Console](screenshots/firehose-console.png)

*Figure 3. Amazon Data Firehose delivery stream with MSK source, Lambda transformation, and Apache Iceberg Tables destination.*

> **Note:** The total deployment time for the full pipeline is approximately 60-70 minutes: CDK stacks (~30 minutes, dominated by MSK cluster creation), VPC connectivity update in Step 5 (~20-30 minutes, rolling broker restart), and Debezium connector creation in Step 6 (~5 minutes). Steps 5 and 6 are performed after the CDK deployment completes.

The MSK cluster requires specific configuration to support both the Debezium connector and Firehose:

- **Dual authentication.** [IAM authentication](https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html) is enabled for Firehose, and unauthenticated access is kept for Debezium. MSK Connect workers use the PLAINTEXT protocol to communicate with brokers, as documented in the [MSK Connect getting started tutorial](https://docs.aws.amazon.com/msk/latest/developerguide/mkc-tutorial-setup.html). This requires the cluster encryption setting `TLS_PLAINTEXT`, which supports both TLS (for Firehose via IAM) and PLAINTEXT (for MSK Connect).
- **VPC connectivity.** [Multi-VPC private connectivity](https://docs.aws.amazon.com/msk/latest/developerguide/aws-access-mult-vpc.html) with IAM is enabled so that Firehose can create an AWS PrivateLink endpoint to the MSK brokers.
- **Topic auto-creation.** A custom MSK configuration sets `auto.create.topics.enable=true`. Without this, Debezium fails with `UNKNOWN_TOPIC_OR_PARTITION` errors because the target topics do not exist when the connector first starts.
- **Cluster resource policy.** A [resource-based policy](https://docs.aws.amazon.com/firehose/latest/dev/writing-with-msk.html) grants the `firehose.amazonaws.com` service principal permission to call `kafka:CreateVpcConnection`.

### Step 5: Enable MSK VPC connectivity, grant Lake Formation permissions, and apply MSK cluster policy

After the CDK deployment completes, enable [multi-VPC private connectivity](https://docs.aws.amazon.com/msk/latest/developerguide/aws-access-mult-vpc.html) with IAM on the MSK cluster. Firehose requires this to create an [AWS PrivateLink](https://docs.aws.amazon.com/vpc/latest/privatelink/what-is-privatelink.html) endpoint to the MSK brokers. This setting cannot be configured during cluster creation and must be applied as an update, which triggers a rolling broker restart (approximately 20-30 minutes).

```bash
# Get the cluster ARN and current version from the CdcMskCluster stack outputs
MSK_ARN=<msk-cluster-arn>
CLUSTER_VERSION=$(aws kafka describe-cluster-v2 \
  --cluster-arn $MSK_ARN \
  --region <your-region> \
  --query 'ClusterInfo.CurrentVersion' --output text)

# Enable VPC connectivity with IAM
aws kafka update-connectivity \
  --cluster-arn $MSK_ARN \
  --current-version $CLUSTER_VERSION \
  --connectivity-info '{"VpcConnectivity":{"ClientAuthentication":{"Sasl":{"Iam":{"Enabled":true}}}}}' \
  --region <your-region>
```

Wait for the cluster state to return to `ACTIVE` before proceeding:

```bash
aws kafka describe-cluster-v2 \
  --cluster-arn $MSK_ARN \
  --region <your-region> \
  --query 'ClusterInfo.State'
```

Next, grant the Firehose IAM role permissions through [AWS Lake Formation](https://docs.aws.amazon.com/lake-formation/latest/dg/what-is-lake-formation.html). S3 Tables uses a sub-catalog format for the `CatalogId` parameter, which differs from the standard [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html). These permissions require a [data lake administrator](https://docs.aws.amazon.com/lake-formation/latest/dg/initial-lf-config.html#create-data-lake-admin) identity.

Grant database-level and table-level permissions to the Firehose role:

```bash
# Grant database-level permissions
aws lakeformation grant-permissions \
  --region <your-region> \
  --principal '{"DataLakePrincipalIdentifier": "<firehose-role-arn>"}' \
  --resource '{"Database": {"CatalogId": "<account-id>:s3tablescatalog/<table-bucket-name>", "Name": "aurora_cdc"}}' \
  --permissions '["ALL"]'

# Grant table-level permissions (wildcard for the tables in the namespace)
aws lakeformation grant-permissions \
  --region <your-region> \
  --principal '{"DataLakePrincipalIdentifier": "<firehose-role-arn>"}' \
  --resource '{"Table": {"CatalogId": "<account-id>:s3tablescatalog/<table-bucket-name>", "DatabaseName": "aurora_cdc", "TableWildcard": {}}}' \
  --permissions '["ALL"]'
```

Note the `CatalogId` format: `<account-id>:s3tablescatalog/<table-bucket-name>`. This is specific to S3 Tables and tells Lake Formation to look up permissions in the S3 Tables catalog rather than the default Glue Data Catalog. For more information, see [Integrating Amazon S3 Tables with AWS analytics services](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-aws.html).

Next, attach a resource-based policy to the MSK cluster that grants the Firehose service principal permission to create [VPC connections](https://docs.aws.amazon.com/msk/latest/developerguide/aws-access-mult-vpc.html):

```bash
aws kafka put-cluster-policy \
  --cluster-arn <msk-cluster-arn> \
  --region <your-region> \
  --policy '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "firehose.amazonaws.com"},
      "Action": ["kafka:CreateVpcConnection", "kafka:GetBootstrapBrokers", "kafka:DescribeClusterV2"],
      "Resource": "<msk-cluster-arn>"
    }]
  }'
```

You can find the `<msk-cluster-arn>` in the `CdcMskCluster` stack outputs from Step 4, and the `<firehose-role-arn>` in the `CdcFirehoseRole` stack outputs.

### Step 6: Create the Debezium connector

With the MSK cluster running and Lake Formation permissions in place, create the Debezium connector using the [MSK Connect](https://docs.aws.amazon.com/msk/latest/developerguide/msk-connect.html) API. The connector reads changes from Aurora PostgreSQL and publishes them to the MSK topic.

First, retrieve the MSK bootstrap servers from the cluster:

```bash
aws kafka get-bootstrap-brokers \
  --cluster-arn <msk-cluster-arn> \
  --region <your-region>
```

Note the `BootstrapBrokerString` value (the PLAINTEXT brokers). Then create the connector:

```bash
aws kafkaconnect create-connector --cli-input-json '{
  "connectorName": "aurora-postgres-debezium-connector",
  "kafkaCluster": {
    "apacheKafkaCluster": {
      "bootstrapServers": "<bootstrap-servers>",
      "vpc": {
        "subnets": ["<subnet-1>", "<subnet-2>"],
        "securityGroups": ["<msk-security-group-id>"]
      }
    }
  },
  "kafkaClusterClientAuthentication": {"authenticationType": "NONE"},
  "kafkaClusterEncryptionInTransit": {"encryptionType": "PLAINTEXT"},
  "kafkaConnectVersion": "2.7.1",
  "plugins": [{"customPlugin": {"customPluginArn": "<custom-plugin-arn>", "revision": 1}}],
  "serviceExecutionRoleArn": "<msk-connect-service-role-arn>",
  "capacity": {"provisionedCapacity": {"mcuCount": 2, "workerCount": 2}},
  "workerConfiguration": {"workerConfigurationArn": "<worker-config-arn>", "revision": 1},
  "connectorConfiguration": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "<aurora-cluster-endpoint>",
    "database.port": "5432",
    "database.user": "<db-user>",
    "database.password": "<db-password>",
    "database.dbname": "<database-name>",
    "database.server.name": "aurora_cdc",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "dbz_publication",
    "table.include.list": "public.orders,public.products",
    "topic.prefix": "aurora.cdc",
    "schema.history.internal.kafka.topic": "schema-changes.aurora",
    "schema.history.internal.kafka.bootstrap.servers": "<bootstrap-servers>",
    "decimal.handling.mode": "string",
    "time.precision.mode": "adaptive_time_microseconds",
    "tombstones.on.delete": "false",
    "snapshot.mode": "initial",
    "publication.autocreate.mode": "filtered",
    "transforms": "Reroute",
    "transforms.Reroute.type": "io.debezium.transforms.ByLogicalTableRouter",
    "transforms.Reroute.topic.regex": "aurora\\\\.cdc\\\\.public\\\\.(.*)",
    "transforms.Reroute.topic.replacement": "aurora.cdc.all-tables"
  },
  "logDelivery": {
    "workerLogDelivery": {
      "cloudWatchLogs": {
        "enabled": true,
        "logGroup": "/aws/msk-connect/aurora-cdc-debezium"
      }
    }
  }
}'
```

The `<msk-security-group-id>` and `<msk-connect-service-role-arn>` can be found in the `CdcMskCluster` and `CdcMskConnectIam` stack outputs respectively. The `ByLogicalTableRouter` [Single Message Transform](https://debezium.io/documentation/reference/stable/transformations/topic-routing.html) routes CDC events from the monitored tables into a single topic (`aurora.cdc.all-tables`).

### Step 7: Verify the Debezium connector

After creating the connector, verify that it is running and has completed its initial snapshot.

```bash
aws kafkaconnect list-connectors --region <your-region> \
  --query 'connectors[?connectorName==`aurora-postgres-debezium-connector`].{Name:connectorName,State:connectorState}' \
  --output table
```

The connector state should show `RUNNING`, as shown in the following figure.

![MSK Connect](screenshots/msk-connect.png)

*Figure 4. Debezium connector running on Amazon MSK Connect.*

Check the CloudWatch Logs to confirm the snapshot completed:

```bash
aws logs tail /aws/msk-connect/aurora-cdc-debezium --follow --region <your-region>
```

You should see messages indicating the transition to streaming mode:

```
Finished exporting 0 records for table 'public.orders' (1 of 2 tables)
Finished exporting 0 records for table 'public.products' (2 of 2 tables)
Snapshot completed
Starting streaming
```

If the tables were empty when the connector started, the export count is 0. If you had existing data, the snapshot captures the existing rows as `r` (read) operations, which the Lambda function maps to `insert` operations in the Iceberg tables.

Verify that the Firehose delivery stream is active:

```bash
aws firehose describe-delivery-stream \
  --delivery-stream-name msk-to-s3tables-firehose \
  --region <your-region> \
  --query 'DeliveryStreamDescription.DeliveryStreamStatus'
```

The status should return `ACTIVE`.

### Step 8: Test the pipeline

Insert test data into the Aurora PostgreSQL source tables. Each insert triggers a CDC event that flows through the pipeline: Aurora WAL to Debezium to MSK topic to Firehose to Lambda transform to S3 Tables.

```sql
-- Insert orders
INSERT INTO public.orders (customer_id, order_date, total_amount, status)
VALUES
  (1, '2026-01-20', 299.99, 'shipped'),
  (2, '2026-01-21', 149.50, 'processing'),
  (1, '2026-01-22', 89.99, 'delivered');

-- Insert products
INSERT INTO public.products (product_name, category, price, stock_quantity)
VALUES
  ('Wireless Headphones', 'Electronics', 79.99, 150),
  ('Running Shoes', 'Sports', 129.99, 75),
  ('Coffee Maker', 'Kitchen', 49.99, 200);
```

This creates 6 records across 2 tables. Each record generates a Debezium CDC event with operation type `c` (create), which the Lambda function maps to an `insert` operation in the corresponding Iceberg table.

### Step 9: Verify data delivery

Check the Firehose `IncomingRecords` metric to confirm records are flowing through the delivery stream:

```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/Firehose \
  --metric-name IncomingRecords \
  --dimensions Name=DeliveryStreamName,Value=msk-to-s3tables-firehose \
  --start-time $(date -u -v-10M +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 60 --statistics Sum \
  --region <your-region>
```

You should see a `Sum` value of 6 or more. If the value is 0, wait another minute and retry - there can be a short delay between MSK topic delivery and Firehose metric reporting.

If records are not appearing, check the Firehose error output in the backup S3 bucket and the Lambda function's CloudWatch Logs for transformation errors.

### Step 10: Query data using Amazon Athena

With data delivered to S3 Tables, you can query the Iceberg tables using [Amazon Athena](https://docs.aws.amazon.com/athena/latest/ug/what-is.html). S3 Tables integrates with the [AWS Glue Data Catalog](https://docs.aws.amazon.com/glue/latest/dg/catalog-and-crawler.html) as a sub-catalog, so you reference tables using the S3 Tables catalog format.

Open the Athena console, select the **AwsDataCatalog** data source, and run the following queries:

```sql
SELECT * FROM "s3tablescatalog/<table-bucket-name>"."aurora_cdc"."products" LIMIT 10;
SELECT * FROM "s3tablescatalog/<table-bucket-name>"."aurora_cdc"."orders" LIMIT 10;
```

Replace `<table-bucket-name>` with your S3 table bucket name. You should see the records from the initial snapshot that Debezium captured when the connector started.

The following figures show the initial state of both tables as queried through Athena. At this point, the products table contains seven records and the orders table contains seven records, captured during the Debezium initial snapshot.

![Products Initial](screenshots/products-initial.png)

*Figure 5. Initial state of the products table in Amazon Athena, showing seven records captured from Aurora PostgreSQL through the CDC pipeline.*

![Orders Initial](screenshots/orders-initial.png)

*Figure 6. Initial state of the orders table in Amazon Athena, showing seven records captured from Aurora PostgreSQL through the CDC pipeline.*

Now test that update and delete operations propagate correctly. Run the following statements in Aurora:

```sql
-- Insert new records
INSERT INTO public.products (product_name, category, price, stock_quantity)
VALUES ('Bluetooth Speaker', 'Electronics', 129.99, 90), ('Standing Desk', 'Furniture', 799.99, 20);

INSERT INTO public.orders (customer_id, order_date, total_amount, status)
VALUES (201, '2026-04-03', 149.99, 'NEW'), (202, '2026-04-03', 249.50, 'NEW'), (203, '2026-04-03', 79.90, 'NEW');

-- Update existing records
UPDATE public.products SET stock_quantity = 30, price = 549.99 WHERE product_name = 'Ergonomic Chair';
UPDATE public.orders SET status = 'DELIVERED' WHERE order_id = 201;

-- Delete a record
DELETE FROM public.products WHERE product_name = 'Test Widget';
```

Wait for the changes to propagate through the pipeline, then query Athena again. The following figures show the results after the insert, update, and delete operations have been applied.

In the products table, the Test Widget record (product_id 100) is no longer present - it was removed by the delete operation. The Ergonomic Chair row now reflects the updated price (549.99) and stock quantity (30). Two new records, Bluetooth Speaker and Standing Desk, appear with a later `created_at` timestamp, confirming they were inserted after the initial snapshot.

![Products CDC](screenshots/products-cdc.png)

*Figure 7. Products table after CDC operations. The Ergonomic Chair, Headphones, and Desk Lamp rows reflect updated values. Bluetooth Speaker and Standing Desk are newly inserted records. The Test Widget record has been removed by the delete operation.*

In the orders table, order 100 now shows a status of SHIPPED and order 201 shows DELIVERED, reflecting the update operations. Three new orders (301, 302, 303) appear with status NEW and a later timestamp, confirming they were inserted after the initial load.

![Orders CDC](screenshots/order-cdc.png)

*Figure 8. Orders table after CDC operations. Orders 100 and 201 reflect updated status values. Orders 301, 302, and 303 are newly inserted records.*

This confirms that the pipeline correctly handles the three CDC operation types: inserts, updates, and deletes are captured from the Aurora WAL by Debezium, routed through the single MSK topic, transformed by the Lambda function, and applied as row-level Iceberg operations by Firehose.

S3 Tables handles [compaction](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-maintenance-compaction.html) and [snapshot management](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-maintenance-snapshots.html) for Iceberg tables automatically, including compaction of small data files and expiration of old snapshots. You do not need to run manual maintenance operations.

You can also use Iceberg's [time travel](https://docs.aws.amazon.com/athena/latest/ug/querying-iceberg-table-data.html) capability to query the table as it existed before the updates:

```sql
SELECT * FROM "s3tablescatalog/<table-bucket-name>"."aurora_cdc"."orders"
FOR TIMESTAMP AS OF current_timestamp - interval '5' minute;
```

This returns the original data before the update, demonstrating the time travel capability that Apache Iceberg provides through S3 Tables.

## Cleaning up

To avoid ongoing charges, delete the resources in reverse dependency order.

Delete the CDK stacks:

```bash
cd cdk
npx cdk --app "npx ts-node bin/app-v2.ts" destroy --all
```

Delete the Debezium custom plugin and worker configuration that were created via the AWS CLI in Step 2:

```bash
aws kafkaconnect delete-custom-plugin --custom-plugin-arn <plugin-arn>
aws kafkaconnect delete-worker-configuration --worker-configuration-arn <worker-config-arn>
```

Clean up the Aurora PostgreSQL replication resources:

```sql
SELECT pg_drop_replication_slot('debezium_slot');
DROP PUBLICATION dbz_publication;
```

> **Important:** The replication slot (`debezium_slot`) was created automatically by Debezium. If you plan to redeploy the pipeline later, you do not need to drop the slot and publication. However, the replication slot continues to retain WAL segments while the connector is not running, which can increase storage usage on the Aurora cluster. The MSK cluster is the largest cost component of this solution and cannot be paused - it can only be deleted and recreated.

## Conclusion

In this post, we showed you how to build a near real-time CDC pipeline from Aurora PostgreSQL to Apache Iceberg tables in Amazon S3 Tables. The key architectural decisions and benefits include:

- **Single-topic routing.** The Debezium `ByLogicalTableRouter` SMT routes CDC events from multiple tables through one MSK topic and one Firehose stream, reducing VPC connection costs and operational complexity.
- **Lambda-based multi-table routing.** The `otfMetadata` block directs each record to the correct Iceberg table, so a single Firehose stream can perform inserts, updates, and deletes across multiple destination tables.
- **Fully managed pipeline.** MSK Connect runs Debezium without infrastructure management, and Firehose handles delivery with automatic retries and error handling. S3 Tables manages Iceberg snapshot management and compaction automatically.
- **CDC semantics preserved.** The Lambda transform maps Debezium operations to Iceberg insert, update, and delete operations, keeping the lakehouse synchronized with the source database.
- **Private connectivity.** Firehose connects to MSK through AWS PrivateLink, keeping traffic within the AWS network.
- **Governed access.** You can use Lake Formation to control access to the Iceberg tables with fine-grained permissions for downstream consumers.
- **Cross-domain analytics.** Data from multiple isolated Aurora clusters can be unified in a single S3 Tables namespace, letting you join and analyze datasets that were previously siloed.
- **Infrastructure as code.** Six AWS CDK stacks deploy the core pipeline infrastructure, with Lake Formation permissions, MSK cluster policy, and Debezium connector configured through documented CLI steps.

To get started, clone the [sample repository](https://github.com/aws-samples/sample-aurora-cdc-s3tables) and follow the walkthrough steps. For more information about the services used in this solution, see the [Amazon MSK Developer Guide](https://docs.aws.amazon.com/msk/latest/developerguide/what-is-msk.html), [Amazon Data Firehose Developer Guide](https://docs.aws.amazon.com/firehose/latest/dev/what-is-this-service.html), and [Amazon S3 Tables User Guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables.html).

We encourage you to try this solution and adapt it to your own CDC workloads. If you have questions or feedback, leave a comment on this post.

## About the authors

*Author bios go here.*

## Related posts

- [Build a data lake for streaming data with Amazon S3 Tables and Amazon Data Firehose](https://aws.amazon.com/blogs/storage/build-a-data-lake-for-streaming-data-with-amazon-s3-tables-and-amazon-data-firehose/)
- [Stream CDC into an Amazon S3 data lake in Apache Iceberg format with AWS Glue Streaming and Amazon MSK Connect](https://aws.amazon.com/blogs/big-data/stream-change-data-into-an-amazon-s3-data-lake-in-apache-iceberg-format-with-aws-glue-streaming-and-amazon-msk-connect/)
- [Introducing Amazon MSK Connect - Stream Data to and from Your Apache Kafka Clusters Using Managed Connectors](https://aws.amazon.com/blogs/aws/introducing-amazon-msk-connect-stream-data-to-and-from-your-apache-kafka-clusters-using-managed-connectors/)
