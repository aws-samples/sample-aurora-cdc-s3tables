#!/bin/bash
set -e

PROFILE="achintan-secondary"
REGION="us-east-1"

echo "=== MSK Connect Deployment for Aurora CDC ==="
echo "Profile: ${PROFILE}"
echo "Region: ${REGION}"
echo ""

# Step 1: Deploy IAM roles and S3 bucket
echo "Step 1: Deploying IAM roles and S3 bucket..."
AWS_PROFILE=${PROFILE} aws cloudformation create-stack \
  --stack-name aurora-cdc-msk-iam \
  --template-body file://msk-connect-iam.yaml \
  --capabilities CAPABILITY_NAMED_IAM \
  --region ${REGION}

echo "Waiting for IAM stack to complete..."
AWS_PROFILE=${PROFILE} aws cloudformation wait stack-create-complete \
  --stack-name aurora-cdc-msk-iam \
  --region ${REGION}

echo "✅ IAM stack deployed"
echo ""

# Step 2: Deploy MSK cluster
echo "Step 2: Deploying MSK cluster (this takes ~20 minutes)..."
AWS_PROFILE=${PROFILE} aws cloudformation create-stack \
  --stack-name aurora-cdc-msk-cluster \
  --template-body file://msk-cluster.yaml \
  --region ${REGION}

echo "Waiting for MSK cluster to complete..."
AWS_PROFILE=${PROFILE} aws cloudformation wait stack-create-complete \
  --stack-name aurora-cdc-msk-cluster \
  --region ${REGION}

echo "✅ MSK cluster deployed"
echo ""

# Step 3: Get MSK cluster details
echo "Step 3: Retrieving MSK cluster details..."
MSK_CLUSTER_ARN=$(AWS_PROFILE=${PROFILE} aws cloudformation describe-stacks \
  --stack-name aurora-cdc-msk-cluster \
  --region ${REGION} \
  --query 'Stacks[0].Outputs[?OutputKey==`MSKClusterArn`].OutputValue' \
  --output text)

MSK_BOOTSTRAP=$(AWS_PROFILE=${PROFILE} aws kafka get-bootstrap-brokers \
  --cluster-arn ${MSK_CLUSTER_ARN} \
  --region ${REGION} \
  --query 'BootstrapBrokerString' \
  --output text)

MSK_SG=$(AWS_PROFILE=${PROFILE} aws cloudformation describe-stacks \
  --stack-name aurora-cdc-msk-cluster \
  --region ${REGION} \
  --query 'Stacks[0].Outputs[?OutputKey==`MSKSecurityGroupId`].OutputValue' \
  --output text)

echo "MSK Cluster ARN: ${MSK_CLUSTER_ARN}"
echo "Bootstrap Servers: ${MSK_BOOTSTRAP}"
echo ""

# Step 4: Build and upload Debezium plugin
echo "Step 4: Building Debezium plugin..."
./build-debezium-plugin.sh

# Step 5: Register custom plugin
echo "Step 5: Registering custom plugin with MSK Connect..."
BUCKET_NAME=$(AWS_PROFILE=${PROFILE} aws cloudformation describe-stacks \
  --stack-name aurora-cdc-msk-iam \
  --region ${REGION} \
  --query 'Stacks[0].Outputs[?OutputKey==`MSKConnectPluginBucketName`].OutputValue' \
  --output text)

PLUGIN_ARN=$(AWS_PROFILE=${PROFILE} aws kafkaconnect create-custom-plugin \
  --custom-plugin-name debezium-postgres-connector \
  --content-type ZIP \
  --location s3Location={bucketArn=arn:aws:s3:::${BUCKET_NAME},fileKey=plugins/debezium-postgres-connector.zip} \
  --region ${REGION} \
  --query 'CustomPluginArn' \
  --output text)

echo "Waiting for plugin to become ACTIVE..."
while true; do
  STATE=$(AWS_PROFILE=${PROFILE} aws kafkaconnect describe-custom-plugin \
    --custom-plugin-arn ${PLUGIN_ARN} \
    --region ${REGION} \
    --query 'CustomPluginState' \
    --output text)
  
  if [ "$STATE" == "ACTIVE" ]; then
    break
  fi
  echo "Plugin state: ${STATE}. Waiting..."
  sleep 10
done

echo "✅ Custom plugin registered: ${PLUGIN_ARN}"
echo ""

# Step 6: Setup PostgreSQL for CDC
echo "Step 6: Setting up PostgreSQL replication..."
echo "Run these SQL commands on Aurora:"
echo ""
echo "SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');"
echo "CREATE PUBLICATION dbz_publication FOR TABLE public.customers, public.orders, public.products;"
echo ""
read -p "Press Enter after running the SQL commands..."

# Step 7: Deploy Debezium connector
echo "Step 7: Deploying Debezium connector..."
SECRET_ARN=$(AWS_PROFILE=${PROFILE} aws cloudformation describe-stacks \
  --stack-name aurora-cdc-msk-iam \
  --region ${REGION} \
  --query 'Stacks[0].Outputs[?OutputKey==`AuroraCredentialsSecretArn`].OutputValue' \
  --output text)

SERVICE_ROLE=$(AWS_PROFILE=${PROFILE} aws cloudformation describe-stacks \
  --stack-name aurora-cdc-msk-iam \
  --region ${REGION} \
  --query 'Stacks[0].Outputs[?OutputKey==`MSKConnectServiceRoleArn`].OutputValue' \
  --output text)

AWS_PROFILE=${PROFILE} aws cloudformation create-stack \
  --stack-name aurora-cdc-msk-connector \
  --template-body file://msk-connect-debezium.yaml \
  --parameters \
    ParameterKey=MSKClusterArn,ParameterValue=${MSK_CLUSTER_ARN} \
    ParameterKey=MSKBootstrapServers,ParameterValue=${MSK_BOOTSTRAP} \
    ParameterKey=CustomPluginArn,ParameterValue=${PLUGIN_ARN} \
    ParameterKey=ServiceRoleArn,ParameterValue=${SERVICE_ROLE} \
    ParameterKey=SecurityGroupId,ParameterValue=${MSK_SG} \
    ParameterKey=AuroraSecretArn,ParameterValue=${SECRET_ARN} \
  --region ${REGION}

echo "Waiting for connector deployment..."
AWS_PROFILE=${PROFILE} aws cloudformation wait stack-create-complete \
  --stack-name aurora-cdc-msk-connector \
  --region ${REGION}

echo ""
echo "=== Deployment Complete ==="
echo "MSK Cluster: ${MSK_CLUSTER_ARN}"
echo "Bootstrap Servers: ${MSK_BOOTSTRAP}"
echo "Connector: aurora-postgres-debezium-connector"
echo ""
echo "CDC topics will be created as: aurora.cdc.public.customers, aurora.cdc.public.orders, aurora.cdc.public.products"
echo ""
echo "Monitor connector status:"
echo "AWS_PROFILE=${PROFILE} aws kafkaconnect describe-connector --connector-arn <connector-arn> --region ${REGION}"
