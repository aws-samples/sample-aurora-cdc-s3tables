#!/bin/bash
set -e

PROFILE="achintan-secondary"
REGION="us-east-1"

echo "=== Deploying Debezium Connector via AWS CLI ==="

# Create worker configuration
echo "Creating worker configuration..."
WORKER_CONFIG=$(cat <<'EOF'
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=false
value.converter.schemas.enable=false
EOF
)

WORKER_ARN=$(AWS_PROFILE=${PROFILE} aws kafkaconnect create-worker-configuration \
  --name debezium-worker-config \
  --description "Worker configuration for Debezium" \
  --properties-file-content "${WORKER_CONFIG}" \
  --region ${REGION} \
  --query 'workerConfigurationArn' \
  --output text)

echo "Worker Configuration ARN: ${WORKER_ARN}"

# Wait a moment for worker config to be ready
sleep 5

# Create connector
echo "Creating Debezium connector..."
AWS_PROFILE=${PROFILE} aws kafkaconnect create-connector \
  --connector-name aurora-postgres-debezium-connector \
  --connector-description "Debezium CDC connector for Aurora PostgreSQL" \
  --kafka-cluster '{
    "apacheKafkaCluster": {
      "bootstrapServers": "b-2.auroracdccluster.5yymx6.c18.kafka.us-east-1.amazonaws.com:9092,b-1.auroracdccluster.5yymx6.c18.kafka.us-east-1.amazonaws.com:9092",
      "vpc": {
        "subnets": ["subnet-0215dd02b9e3edbde", "subnet-0ad591a7a23c94b6c"],
        "securityGroups": ["sg-0957e9b7f4d80c072"]
      }
    }
  }' \
  --kafka-cluster-client-authentication '{"authenticationType": "NONE"}' \
  --kafka-cluster-encryption-in-transit '{"encryptionType": "PLAINTEXT"}' \
  --kafka-connect-version "2.7.1" \
  --plugins '[{
    "customPlugin": {
      "customPluginArn": "arn:aws:kafkaconnect:us-east-1:063337766236:custom-plugin/debezium-postgres-connector/5fbf9bb4-d01b-40c6-835b-18ad7e5520af-4",
      "revision": 1
    }
  }]' \
  --service-execution-role-arn "arn:aws:iam::063337766236:role/msk-connect-service-role" \
  --capacity '{
    "provisionedCapacity": {
      "mcuCount": 2,
      "workerCount": 2
    }
  }' \
  --worker-configuration '{
    "workerConfigurationArn": "'"${WORKER_ARN}"'",
    "revision": 1
  }' \
  --connector-configuration '{
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "2",
    "database.hostname": "aurora-cdc-rds-auroracluster-l1vzx1j2gxdb.cluster-crkeeemtkh5z.us-east-1.rds.amazonaws.com",
    "database.port": "5432",
    "database.user": "dbadmin",
    "database.password": "<your-db-password>",
    "database.dbname": "cdcdemo",
    "database.server.name": "aurora_cdc",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "dbz_publication",
    "table.include.list": "public.customers,public.orders,public.products",
    "topic.prefix": "aurora.cdc",
    "schema.history.internal.kafka.topic": "schema-changes.aurora",
    "schema.history.internal.kafka.bootstrap.servers": "b-2.auroracdccluster.5yymx6.c18.kafka.us-east-1.amazonaws.com:9092,b-1.auroracdccluster.5yymx6.c18.kafka.us-east-1.amazonaws.com:9092",
    "decimal.handling.mode": "string",
    "time.precision.mode": "adaptive_time_microseconds",
    "tombstones.on.delete": "false",
    "snapshot.mode": "initial",
    "publication.autocreate.mode": "filtered"
  }' \
  --log-delivery '{
    "workerLogDelivery": {
      "cloudWatchLogs": {
        "enabled": true,
        "logGroup": "/aws/msk-connect/aurora-cdc-debezium"
      }
    }
  }' \
  --region ${REGION}

echo ""
echo "✅ Connector creation initiated!"
echo ""
echo "Monitor status with:"
echo "AWS_PROFILE=${PROFILE} aws kafkaconnect list-connectors --region ${REGION}"
