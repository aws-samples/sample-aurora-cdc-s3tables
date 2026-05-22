# Real-time CDC from Aurora PostgreSQL to Amazon S3 Tables using Debezium and Firehose

This repository contains the companion code for the AWS Big Data Blog post of the same name.

## Overview

A near real-time CDC pipeline that streams row-level changes from Amazon Aurora PostgreSQL into Apache Iceberg tables in Amazon S3 Tables. The pipeline uses Debezium on Amazon MSK Connect, Amazon MSK, AWS Lambda, and Amazon Data Firehose.

![Architecture Diagram](aurora-cdc-s3tables-architecture.png)

## How it works

1. **Aurora PostgreSQL** captures row-level changes using logical replication
2. **Debezium (MSK Connect)** reads the WAL and publishes CDC events to a single MSK topic via the `ByLogicalTableRouter` SMT
3. **Amazon Data Firehose** consumes from MSK over PrivateLink and invokes a Lambda transform
4. **AWS Lambda** flattens the Debezium envelope, maps operation types (insert/update/delete), and sets `otfMetadata` routing per table
5. **Firehose** writes transformed records to Apache Iceberg tables in S3 Tables
6. **AWS Lake Formation** manages fine-grained access; query with Athena, Redshift, or SageMaker Unified Studio

A single Lambda function routes CDC events from multiple source tables through one Firehose delivery stream, eliminating per-table infrastructure.

## Prerequisites

- AWS account with [S3 Tables integration](https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-aws.html) enabled
- Aurora PostgreSQL cluster with `rds.logical_replication = 1`
- VPC with at least two subnets in different AZs
- S3 bucket for Debezium plugin upload and Firehose failed record backup
- AWS CDK v2, Node.js 18+, AWS CLI v2

## Deployment

### Step 1: Enable CDC in Aurora PostgreSQL

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

-- Publication only; Debezium creates the replication slot automatically
CREATE PUBLICATION dbz_publication FOR TABLE public.orders, public.products;
```

### Step 2: Build and register the Debezium plugin

```bash
DEBEZIUM_VERSION=2.7.3.Final
curl -LO "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz"

mkdir -p debezium-plugin
tar -xzf debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz -C debezium-plugin/
cd debezium-plugin && zip -r ../debezium-postgres-connector.zip . && cd ..

aws s3 cp debezium-postgres-connector.zip s3://<your-plugin-bucket>/plugins/

aws kafkaconnect create-custom-plugin \
  --custom-plugin-name debezium-postgres-connector \
  --content-type ZIP \
  --location "s3Location={bucketArn=arn:aws:s3:::<your-plugin-bucket>,fileKey=plugins/debezium-postgres-connector.zip}"

aws kafkaconnect create-worker-configuration \
  --name debezium-worker-config \
  --properties-file-content "$(echo -n 'key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false' | base64)"
```

### Step 3: Configure and deploy CDK stacks

Update `cdk/lib/v2/config.ts` with your environment values, then deploy:

```bash
cd cdk
npm install
npx cdk --app "npx ts-node bin/app-v2.ts" deploy --all
```

| Stack | What it creates |
|-------|-----------------|
| `CdcMskCluster` | MSK cluster with IAM + unauthenticated auth, `auto.create.topics.enable=true`, security groups |
| `CdcMskConnectIam` | MSK Connect service role, CloudWatch log group |
| `CdcS3Tables` | S3 table bucket, `aurora_cdc` namespace, 2 Iceberg tables |
| `CdcLambdaTransform` | Lambda function for CDC transformation and routing |
| `CdcFirehoseRole` | Firehose IAM role with MSK, S3 Tables, Glue, Lake Formation permissions |
| `CdcFirehose` | Firehose delivery stream (MSK source, Lambda transform, Iceberg destination) |

### Step 4: Post-deployment CLI steps

These require a Lake Formation administrator identity and cannot be automated through CDK:

```bash
# Enable VPC connectivity for Firehose PrivateLink
aws kafka update-connectivity \
  --cluster-arn <msk-cluster-arn> \
  --current-version <version> \
  --connectivity-info '{"VpcConnectivity":{"ClientAuthentication":{"Sasl":{"Iam":{"Enabled":true}}}}}'

# Grant Lake Formation permissions
aws lakeformation grant-permissions \
  --principal '{"DataLakePrincipalIdentifier": "<firehose-role-arn>"}' \
  --resource '{"Database": {"CatalogId": "<account-id>:s3tablescatalog/<table-bucket-name>", "Name": "aurora_cdc"}}' \
  --permissions '["ALL"]'

aws lakeformation grant-permissions \
  --principal '{"DataLakePrincipalIdentifier": "<firehose-role-arn>"}' \
  --resource '{"Table": {"CatalogId": "<account-id>:s3tablescatalog/<table-bucket-name>", "DatabaseName": "aurora_cdc", "TableWildcard": {}}}' \
  --permissions '["ALL"]'

# Apply MSK cluster resource policy
aws kafka put-cluster-policy \
  --cluster-arn <msk-cluster-arn> \
  --policy '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"firehose.amazonaws.com"},"Action":["kafka:CreateVpcConnection","kafka:GetBootstrapBrokers","kafka:DescribeClusterV2"],"Resource":"<msk-cluster-arn>"}]}'
```

### Step 5: Create the Debezium connector

See `blog-post.md` Step 6 for the full connector JSON configuration with the `ByLogicalTableRouter` SMT.

## Debezium event transformation

The Lambda converts Debezium CDC format into Firehose Iceberg format:

```json
// Debezium input
{"op": "c", "before": null, "after": {"order_id": 1, "total_amount": 299.99}, "source": {"table": "orders"}}

// Lambda output
{"recordId": "...", "result": "Ok", "kafkaRecordValue": "<base64 flattened row>", "metadata": {"otfMetadata": {"destinationDatabaseName": "aurora_cdc", "destinationTableName": "orders", "operation": "insert"}}}
```

| Debezium op | Firehose operation |
|-------------|-------------------|
| `c` | `insert` |
| `u` | `update` |
| `d` | `delete` |
| `r` (snapshot) | `insert` |

## Cleanup

```bash
# Delete Debezium connector
aws kafkaconnect delete-connector --connector-arn <arn>
aws kafkaconnect delete-custom-plugin --custom-plugin-arn <arn>
aws kafkaconnect delete-worker-configuration --worker-configuration-arn <arn>

# Delete CDK stacks
cd cdk
npx cdk --app "npx ts-node bin/app-v2.ts" destroy --all

# Drop replication slot and publication
SELECT pg_drop_replication_slot('debezium_slot');
DROP PUBLICATION dbz_publication;
```

## Security

See the blog post for detailed security considerations. Key points:

- Database credentials stored in Secrets Manager
- MSK uses IAM auth for Firehose, PrivateLink for private connectivity
- MSK Connect workers run in private subnets with security group isolation
- S3 Tables access controlled through Lake Formation
- Firehose backup bucket enforces SSL and encryption
- All IAM roles follow least-privilege principles

## License

MIT-0. See [LICENSE](LICENSE).
