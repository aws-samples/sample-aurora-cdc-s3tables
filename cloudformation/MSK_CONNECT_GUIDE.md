# MSK Connect Deployment Guide

## Architecture

```
Aurora PostgreSQL (cdcdemo)
    ↓ (Debezium CDC)
Amazon MSK Connect
    ↓ (Kafka Topics)
Amazon MSK Cluster
    ↓ (Consumer)
Flink/Processor → S3 Tables
```

## Prerequisites

- Aurora PostgreSQL cluster with logical replication enabled ✅
- VPC with at least 2 subnets in different AZs ✅
- AWS CLI configured with `achintan-secondary` profile ✅

## Deployment Steps

### 1. Deploy Infrastructure

Run the automated deployment script:

```bash
cd /Users/achintan/Desktop/ProServe/blog/aurora-cdc-s3Tables/cloudformation
./deploy-msk-connect.sh
```

This will:
1. Create IAM roles and S3 bucket for plugins (~2 min)
2. Deploy MSK cluster with 2 brokers (~20 min)
3. Download and package Debezium connector (~1 min)
4. Register custom plugin with MSK Connect (~2 min)
5. Deploy Debezium connector (~5 min)

**Total time: ~30 minutes**

### 2. Configure PostgreSQL for CDC

Connect to Aurora and run:

```sql
-- Create replication slot
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');

-- Create publication for specific tables
CREATE PUBLICATION dbz_publication FOR TABLE 
  public.customers, 
  public.orders, 
  public.products;

-- Verify
SELECT * FROM pg_replication_slots WHERE slot_name = 'debezium_slot';
SELECT * FROM pg_publication WHERE pubname = 'dbz_publication';
```

### 3. Verify CDC Pipeline

Check connector status:

```bash
AWS_PROFILE=achintan-secondary aws kafkaconnect list-connectors --region us-east-1

# Get connector details
CONNECTOR_ARN=$(AWS_PROFILE=achintan-secondary aws kafkaconnect list-connectors \
  --region us-east-1 \
  --query 'Connectors[?ConnectorName==`aurora-postgres-debezium-connector`].ConnectorArn' \
  --output text)

AWS_PROFILE=achintan-secondary aws kafkaconnect describe-connector \
  --connector-arn ${CONNECTOR_ARN} \
  --region us-east-1
```

Check CloudWatch logs:

```bash
AWS_PROFILE=achintan-secondary aws logs tail /aws/msk-connect/aurora-cdc-debezium \
  --follow \
  --region us-east-1
```

### 4. Test CDC Events

Insert test data in Aurora:

```sql
INSERT INTO customers (customer_id, name, email) 
VALUES (1001, 'Test User', 'test@example.com');

UPDATE customers SET email = 'updated@example.com' WHERE customer_id = 1001;

DELETE FROM customers WHERE customer_id = 1001;
```

Consume Kafka topics to verify events:

```bash
# Get bootstrap servers
MSK_BOOTSTRAP=$(AWS_PROFILE=achintan-secondary aws kafka get-bootstrap-brokers \
  --cluster-arn <MSK_CLUSTER_ARN> \
  --region us-east-1 \
  --query 'BootstrapBrokerString' \
  --output text)

# List topics (requires Kafka client)
kafka-topics.sh --bootstrap-server ${MSK_BOOTSTRAP} --list

# Expected topics:
# - aurora.cdc.public.customers
# - aurora.cdc.public.orders
# - aurora.cdc.public.products
# - schema-changes.aurora
```

## Resources Created

### CloudFormation Stacks

1. **aurora-cdc-msk-iam**
   - IAM role: `msk-connect-service-role`
   - S3 bucket: `msk-connect-plugins-063337766236-us-east-1`
   - Secret: `aurora-cdc-credentials`

2. **aurora-cdc-msk-cluster**
   - MSK cluster: `aurora-cdc-cluster`
   - 2 brokers (kafka.m5.large)
   - Security group: `msk-aurora-cdc-sg`
   - CloudWatch log group: `/aws/msk/aurora-cdc-cluster`

3. **aurora-cdc-msk-connector**
   - Connector: `aurora-postgres-debezium-connector`
   - Worker configuration: `debezium-worker-config`
   - CloudWatch log group: `/aws/msk-connect/aurora-cdc-debezium`

### MSK Connect Plugin

- Name: `debezium-postgres-connector`
- Version: Debezium 2.7.3.Final
- Includes: AWS Secrets Manager Config Provider 1.2.0

## Kafka Topics

CDC events are published to:

- `aurora.cdc.public.customers` - Customer table changes
- `aurora.cdc.public.orders` - Order table changes
- `aurora.cdc.public.products` - Product table changes
- `schema-changes.aurora` - Schema change history

## Configuration Details

### Debezium Connector Settings

```properties
connector.class=io.debezium.connector.postgresql.PostgresConnector
database.hostname=<from-secrets-manager>
database.port=5432
database.dbname=cdcdemo
plugin.name=pgoutput
slot.name=debezium_slot
publication.name=dbz_publication
snapshot.mode=initial
decimal.handling.mode=string
tombstones.on.delete=false
```

### MSK Cluster Settings

- Kafka version: 3.7.0
- Broker type: kafka.m5.large
- Storage: 100 GB EBS per broker
- Encryption: TLS in-transit, at-rest enabled
- Authentication: Unauthenticated (within VPC)

## Cost Estimate

- MSK cluster (2 x kafka.m5.large): ~$240/month
- MSK Connect (2 MCUs): ~$180/month
- Data transfer: Variable
- CloudWatch logs: ~$5/month

**Total: ~$425/month**

## Troubleshooting

### Connector fails to start

Check CloudWatch logs:
```bash
AWS_PROFILE=achintan-secondary aws logs tail /aws/msk-connect/aurora-cdc-debezium \
  --region us-east-1
```

Common issues:
- Replication slot not created
- Publication not created
- Network connectivity (security groups)
- Secrets Manager permissions

### No CDC events in Kafka

1. Verify replication slot is active:
```sql
SELECT * FROM pg_replication_slots WHERE slot_name = 'debezium_slot';
```

2. Check connector status:
```bash
AWS_PROFILE=achintan-secondary aws kafkaconnect describe-connector \
  --connector-arn ${CONNECTOR_ARN} \
  --region us-east-1
```

3. Verify publication includes tables:
```sql
SELECT * FROM pg_publication_tables WHERE pubname = 'dbz_publication';
```

### Replication lag

Monitor WAL lag:
```sql
SELECT 
  slot_name,
  pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS lag
FROM pg_replication_slots
WHERE slot_name = 'debezium_slot';
```

## Cleanup

To remove all resources:

```bash
# Delete connector
AWS_PROFILE=achintan-secondary aws cloudformation delete-stack \
  --stack-name aurora-cdc-msk-connector \
  --region us-east-1

# Delete MSK cluster
AWS_PROFILE=achintan-secondary aws cloudformation delete-stack \
  --stack-name aurora-cdc-msk-cluster \
  --region us-east-1

# Delete IAM resources
AWS_PROFILE=achintan-secondary aws cloudformation delete-stack \
  --stack-name aurora-cdc-msk-iam \
  --region us-east-1

# Drop PostgreSQL resources
DROP PUBLICATION dbz_publication;
SELECT pg_drop_replication_slot('debezium_slot');
```

## Next Steps

1. **Set up Flink consumer** to process CDC events from MSK
2. **Transform data** for S3 Tables schema
3. **Write to S3 Tables** using Iceberg format
4. **Query with Athena** for analytics

See `flink-processor-setup.yaml` for Flink integration (coming next).
