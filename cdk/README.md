# Aurora CDC to S3 Tables — CDK Stacks

## Architecture

```
Aurora PostgreSQL → Debezium (MSK Connect) → MSK Topics → Firehose (MSK source) → Lambda Transform → S3 Tables (Iceberg)
```

## Stacks

| Stack | Description | Resources |
|-------|-------------|-----------|
| `AuroraCdcS3Tables` | S3 Tables bucket, namespace, Iceberg tables | S3 Tables Bucket, Namespace, 3 Tables |
| `AuroraCdcLambdaTransform` | Debezium → Iceberg transformation | Lambda Function |
| `AuroraCdcFirehose` | Firehose delivery (MSK → S3 Tables) | 3 Firehose streams, IAM Role, Backup S3 |
| `AuroraCdcLakeFormation` | Lake Formation permissions | DB + Table permissions for Firehose role |

## Prerequisites

- Existing MSK cluster with Debezium connector writing CDC events
- Aurora PostgreSQL with logical replication enabled

## Deploy

```bash
cd cdk
npm install

# Deploy all stacks in order
cdk deploy AuroraCdcS3Tables
cdk deploy AuroraCdcLambdaTransform
cdk deploy AuroraCdcFirehose
cdk deploy AuroraCdcLakeFormation

# Or deploy everything at once
cdk deploy --all
```

## Destroy

```bash
cdk destroy --all
```
