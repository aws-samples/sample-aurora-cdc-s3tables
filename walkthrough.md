## Walkthrough

The following steps walk you through building the CDC pipeline end to end. The solution uses AWS CDK to deploy the infrastructure, with two manual prerequisites (Aurora database setup and Debezium plugin registration) that must be completed before the CDK deployment.

### Prerequisites

Before you begin, ensure you have the following:

- An AWS account with permissions to create Amazon MSK, Amazon Data Firehose, AWS Lambda, Amazon S3 Tables, AWS Lake Formation, and IAM resources.
- An existing VPC with at least two subnets in different Availability Zones.
- An Aurora PostgreSQL cluster in the same VPC with logical replication enabled. To enable logical replication, set the `rds.logical_replication` parameter to `1` in your Aurora cluster's parameter group and reboot the cluster. For more information, see [Logical replication for Aurora PostgreSQL](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/AuroraPostgreSQL.Replication.Logical.html).
- Aurora database credentials stored in AWS Secrets Manager. Note the secret ARN — you need it for the CDK configuration.
- AWS CDK v2 installed (`npm install -g aws-cdk`).
- Node.js 18+ and npm.
- AWS CLI v2 installed and configured with appropriate credentials.

### Step 1: Configure Aurora PostgreSQL for CDC

Debezium captures changes by reading the PostgreSQL Write-Ahead Log (WAL) through logical replication. Before Debezium can start capturing changes, you need to create the source tables, a logical replication slot, and a publication that tells PostgreSQL which tables to include in the replication stream.

Connect to your Aurora PostgreSQL cluster using your preferred SQL client and run the following statements to create three sample tables:

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

Next, create a logical replication slot and a publication. The replication slot ensures that PostgreSQL retains WAL segments until Debezium has consumed them, preventing data loss. The publication defines which tables are included in the change stream.

```sql
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');
CREATE PUBLICATION dbz_publication FOR TABLE public.orders, public.products;
```

The `pgoutput` plugin is PostgreSQL's native logical decoding output plugin, which is available on Aurora PostgreSQL without any additional extensions.

Verify that both the replication slot and publication were created successfully:

```sql
SELECT * FROM pg_replication_slots WHERE slot_name = 'debezium_slot';
SELECT * FROM pg_publication WHERE pubname = 'dbz_publication';
```

You should see one row returned for each query, confirming the slot and publication are active.

> **Important:** The replication slot retains WAL segments until consumed. If the Debezium connector is stopped for an extended period, WAL segments accumulate and can cause storage issues on the Aurora cluster. Monitor the `ReplicationSlotDiskUsage` CloudWatch metric for your Aurora cluster.

### Step 2: Build and register the Debezium plugin

MSK Connect runs connectors using custom plugins that you upload to Amazon S3. In this step, you download the Debezium PostgreSQL connector, package it as a ZIP file, upload it to S3, and register it with MSK Connect.

First, download and package the Debezium connector:

```bash
# Download Debezium PostgreSQL connector
DEBEZIUM_VERSION=2.7.3.Final
curl -LO "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz"

# Extract and package as ZIP for MSK Connect
mkdir -p debezium-plugin
tar -xzf debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz -C debezium-plugin/
cd debezium-plugin && zip -r ../debezium-postgres-connector.zip . && cd ..
```

Upload the ZIP file to an S3 bucket. This bucket must be in the same Region as your MSK cluster:

```bash
aws s3 cp debezium-postgres-connector.zip s3://<your-plugin-bucket>/plugins/
```

Register the uploaded ZIP as an MSK Connect custom plugin:

```bash
aws kafkaconnect create-custom-plugin \
  --custom-plugin-name debezium-postgres-connector \
  --content-type ZIP \
  --location "s3Location={bucketArn=arn:aws:s3:::<your-plugin-bucket>,fileKey=plugins/debezium-postgres-connector.zip}"
```

The command returns a `customPluginArn`. Note this value — you need it for the CDK configuration.

Next, create a worker configuration. The worker configuration tells MSK Connect how to serialize Kafka messages. We configure JSON output without schemas, which produces clean JSON records that the Lambda transform function can parse:

```bash
aws kafkaconnect create-worker-configuration \
  --name debezium-worker-config \
  --properties-file-content "$(echo -n 'key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false' | base64)"
```

The command returns a `workerConfigurationArn`. Note this value as well.

> **Note:** The custom plugin and worker configuration are created via the CLI because the Debezium connector JARs must be downloaded from the Debezium project and packaged manually. The remaining infrastructure is deployed using CDK in the following steps.

### Step 3: Configure CDK

Clone the sample repository and install dependencies:

```bash
git clone https://github.com/aws-samples/amazon-aurora-cdc-to-s3-tables.git
cd amazon-aurora-cdc-to-s3-tables/cdk
npm install
```

Open `cdk/lib/v2/config.ts` and update the configuration values to match your environment. The configuration file is organized into sections for networking, Aurora, MSK, Debezium, S3 Tables, and Firehose:

```typescript
export const CONFIG = {
  account: '<your-account-id>',
  region: '<your-region>',

  // VPC — must match your Aurora cluster's VPC
  vpcId: '<your-vpc-id>',
  subnetIds: ['<subnet-1>', '<subnet-2>'],
  auroraSecurityGroupId: '<aurora-security-group-id>',

  // Aurora connection details
  auroraEndpoint: '<aurora-cluster-endpoint>',
  auroraPort: '5432',
  auroraDbName: '<database-name>',
  auroraUser: '<db-user>',
  auroraSecretArn: '<secrets-manager-arn>',

  // Debezium — use the ARNs from Step 2
  debeziumPluginArn: '<customPluginArn-from-step-2>',
  debeziumWorkerConfigArn: '<workerConfigurationArn-from-step-2>',
  debeziumPluginBucket: '<your-plugin-bucket-name>',
  debeziumTopicPrefix: 'aurora.cdc',
  debeziumTables: 'public.orders,public.products',

  // S3 Tables — the table bucket name must be globally unique
  s3TablesBucketName: '<your-table-bucket-name>',
  s3TablesNamespace: 'aurora_cdc',
  tables: ['orders', 'products'],
  tableKeys: { orders: 'order_id', products: 'product_id' },

  // Firehose — general purpose S3 bucket for failed record backup
  firehoseBackupBucket: '<your-backup-bucket-name>',
  ...
};
```

Key configuration notes:

- **`auroraSecurityGroupId`** — The security group attached to your Aurora cluster. The CDK creates an MSK security group with ingress rules allowing traffic from this Aurora security group, and a reverse rule allowing MSK Connect workers to reach Aurora on port 5432 for CDC reads.
- **`debeziumTopicPrefix`** — Determines the Kafka topic naming pattern. Debezium creates topics as `<prefix>.public.<table-name>`, but the `ByLogicalTableRouter` SMT reroutes all events to `aurora.cdc.all-tables`.
- **`tableKeys`** — The primary key column for each table. Firehose uses these to match incoming records against existing rows for update and delete operations in the Iceberg tables.
- **`s3TablesBucketName`** — The name for your S3 table bucket. Table bucket names must be globally unique across all AWS accounts, similar to general purpose S3 bucket names.

### Step 4: Deploy the CDK stacks

Deploy all eight stacks with a single command. CDK resolves the dependency order automatically:

```bash
npx cdk --app "npx ts-node bin/app-v2.ts" deploy --all
```

When prompted, review the IAM changes and confirm the deployment. The CDK deploys the following stacks in dependency order:

| Stack | What it creates |
|-------|----------------|
| `CdcMskCluster` | MSK cluster (2× kafka.m5.large brokers) with dual authentication (IAM for Firehose, unauthenticated for Debezium), multi-VPC private connectivity for Firehose PrivateLink, custom configuration with `auto.create.topics.enable=true`, MSK security group with ingress rules for Aurora and self-referencing rules for MSK Connect workers |
| `CdcMskConnectIam` | MSK Connect service execution role with permissions for Kafka cluster operations, VPC networking, S3 plugin access, and Secrets Manager; plugin S3 bucket with SSE encryption and SSL-only policy; CloudWatch log group for connector logs |
| `CdcDebeziumConnector` | Debezium PostgreSQL connector with `ByLogicalTableRouter` Single Message Transform (SMT) that routes CDC events from all three tables to a single Kafka topic (`aurora.cdc.all-tables`); provisioned capacity with 2 MCU × 2 workers |
| `CdcS3Tables` | S3 table bucket, `aurora_cdc` namespace, and two Iceberg tables (`orders`, `products`) with full column schemas matching the Aurora source tables |
| `CdcLambdaTransform` | Lambda function (`firehose-debezium-transform`) that converts Debezium CDC envelope format to flattened JSON, maps operation types, and sets `otfMetadata` routing for multi-table delivery |
| `CdcFirehoseRole` | Firehose IAM role with permissions for MSK (including `kafka:CreateVpcConnection` for PrivateLink), S3 Tables, Glue Data Catalog, Lake Formation, VPC networking, Lambda invocation, and CloudWatch logging |
| `CdcLakeFormation` | Lake Formation database-level and table-level permissions (ALL) for the Firehose role on the S3 Tables sub-catalog; MSK cluster resource policy allowing the Firehose service principal to create VPC connections |
| `CdcFirehose` | Firehose delivery stream with MSK as source (private connectivity via PrivateLink), Lambda processing for CDC transformation and routing, Apache Iceberg Tables as destination with three table configurations, 1 MiB / 60-second buffer hints, and S3 backup bucket for failed records |

> **Note:** The MSK cluster takes approximately 25 minutes to create. The Debezium connector takes approximately 5 minutes after the cluster is ready. You can monitor the deployment progress in the AWS CloudFormation console. The total deployment time is approximately 35–40 minutes.

> **Note:** The Lake Formation permissions use the S3 Tables sub-catalog format (`<account-id>:s3tablescatalog/<table-bucket-name>`) for the `CatalogId` parameter. This format is not supported by the native `AWS::LakeFormation::PrincipalPermissions` CloudFormation resource, so the CDK stack uses an `AwsCustomResource` construct to grant permissions via the AWS SDK.

### Step 5: Verify the Debezium connector

After the CDK deployment completes, verify that the Debezium connector is running and has completed its initial snapshot of the source tables.

Check the connector status:

```bash
aws kafkaconnect list-connectors --region <your-region> \
  --query 'connectors[?connectorName==`aurora-postgres-debezium-connector`].{Name:connectorName,State:connectorState}' \
  --output table
```

You should see output similar to:

```
-------------------------------------------------------------
|                       ListConnectors                       |
+------------------------------------------+----------------+
|                   Name                   |     State      |
+------------------------------------------+----------------+
|  aurora-postgres-debezium-connector      |  RUNNING       |
+------------------------------------------+----------------+
```

Next, check the CloudWatch logs to confirm the connector has completed its initial snapshot and started streaming:

```bash
aws logs tail /aws/msk-connect/aurora-cdc-debezium --follow --region <your-region>
```

You should see messages indicating the snapshot phase completing and the transition to streaming mode:

```
Finished exporting 0 records for table 'public.orders' (1 of 2 tables)
Finished exporting 0 records for table 'public.products' (2 of 2 tables)
Snapshot completed
Starting streaming
```

If the tables were empty when the connector started, the export count is 0. If you had existing data, the snapshot captures all existing rows as `r` (read) operations, which the Lambda function maps to `insert` operations in the Iceberg tables.

You can also verify that the Firehose delivery stream is active:

```bash
aws firehose describe-delivery-stream \
  --delivery-stream-name msk-to-s3tables-firehose \
  --region <your-region> \
  --query 'DeliveryStreamDescription.DeliveryStreamStatus'
```

The status should return `ACTIVE`.

### Step 6: Test the pipeline with sample data

With the pipeline running, insert test data into the Aurora PostgreSQL source tables. Each insert triggers a CDC event that flows through the pipeline: Aurora WAL → Debezium → MSK topic → Firehose → Lambda transform → S3 Tables.

Connect to your Aurora PostgreSQL cluster and run the following inserts:

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

### Step 7: Verify data delivery

Records typically appear in S3 Tables within 60–90 seconds, depending on the Firehose buffer configuration (1 MiB buffer size or 60-second interval, whichever is reached first).

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

You should see a `Sum` value of 6 (or more, if you ran additional inserts). If the value is 0, wait another minute and retry — there is a short delay between MSK topic delivery and Firehose metric reporting.

You can also check the Lambda transform function's invocation metrics to confirm it is processing records:

```bash
aws cloudwatch get-metric-statistics \
  --namespace AWS/Lambda \
  --metric-name Invocations \
  --dimensions Name=FunctionName,Value=firehose-debezium-transform \
  --start-time $(date -u -v-10M +%Y-%m-%dT%H:%M:%S) \
  --end-time $(date -u +%Y-%m-%dT%H:%M:%S) \
  --period 60 --statistics Sum \
  --region <your-region>
```

If records are not appearing, check the Firehose error output in the backup S3 bucket and the Lambda function's CloudWatch logs for transformation errors.

### Step 8: Query data using Amazon Athena

With data delivered to S3 Tables, you can query the Iceberg tables using Amazon Athena. S3 Tables integrates with the AWS Glue Data Catalog as a sub-catalog, so you reference tables using the S3 Tables catalog format.

Open the Athena console, select the **AwsDataCatalog** data source, and run the following queries:

```sql
-- Query orders table
SELECT * FROM "s3tablescatalog/<table-bucket-name>"."aurora_cdc"."orders";

-- Query products table
SELECT * FROM "s3tablescatalog/<table-bucket-name>"."aurora_cdc"."products";
```

Replace `<table-bucket-name>` with your S3 table bucket name. You should see the 3 records in each table that you inserted in Step 6.

Now test that update and delete operations propagate correctly through the pipeline. Run the following statements in Aurora:

```sql
-- Update an order status
UPDATE public.orders SET status = 'delivered' WHERE order_id = 2;

-- Delete a product
DELETE FROM public.products WHERE product_id = 3;
```

Wait 60–90 seconds for the changes to propagate, then query Athena again:

```sql
-- Verify the update: order_id 2 should now show 'delivered'
SELECT order_id, status FROM "s3tablescatalog/<table-bucket-name>"."aurora_cdc"."orders";

-- Verify the delete: product_id 3 (Coffee Maker) should be removed
SELECT product_id, product_name FROM "s3tablescatalog/<table-bucket-name>"."aurora_cdc"."products";
```

The orders table should show `delivered` for order 2, and the products table should contain only 2 records (Wireless Headphones and Running Shoes). This confirms that the pipeline correctly handles all three CDC operation types: inserts, updates, and deletes.

> **Note:** Because S3 Tables provides automatic compaction and snapshot management for Iceberg tables, you do not need to run manual maintenance operations. S3 Tables handles compaction of small files and expiration of old snapshots automatically.

You can also use Iceberg's time travel capability to query the table as it existed before the updates:

```sql
-- Query the orders table as of 5 minutes ago
SELECT * FROM "s3tablescatalog/<table-bucket-name>"."aurora_cdc"."orders"
FOR TIMESTAMP AS OF current_timestamp - interval '5' minute;
```

This returns the original data before the update, demonstrating the time travel capability that Iceberg provides through S3 Tables.

## Cleaning up

To avoid ongoing charges, delete the resources in reverse dependency order. The MSK cluster (~$550/month) is the largest cost driver and cannot be paused — only deleted.

Delete the CDK stacks:

```bash
cd cdk
npx cdk --app "npx ts-node bin/app-v2.ts" destroy --all
```

When prompted, confirm the deletion of each stack. The stacks are deleted in reverse dependency order automatically.

Delete the Debezium custom plugin and worker configuration that were created via CLI in Step 2:

```bash
aws kafkaconnect delete-custom-plugin --custom-plugin-arn <plugin-arn>
aws kafkaconnect delete-worker-configuration --worker-configuration-arn <worker-config-arn>
```

Finally, clean up the Aurora PostgreSQL replication resources to stop WAL retention:

```sql
-- Connect to Aurora PostgreSQL
SELECT pg_drop_replication_slot('debezium_slot');
DROP PUBLICATION dbz_publication;
```

> **Important:** If you plan to redeploy the pipeline later, you do not need to drop the replication slot and publication. However, be aware that the replication slot continues to retain WAL segments while the connector is not running, which can increase storage usage on the Aurora cluster.
