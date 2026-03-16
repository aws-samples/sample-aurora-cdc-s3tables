#!/bin/bash
set -e

# Configuration
DEBEZIUM_VERSION="2.7.3.Final"
SECRETS_PROVIDER_VERSION="1.2.0"
PLUGIN_NAME="debezium-postgres-connector"
REGION="us-east-1"
PROFILE="achintan-secondary"

echo "=== Debezium PostgreSQL Connector Plugin Builder ==="
echo "Debezium Version: ${DEBEZIUM_VERSION}"
echo "Secrets Provider Version: ${SECRETS_PROVIDER_VERSION}"
echo ""

# Get S3 bucket from CloudFormation
BUCKET_NAME=$(AWS_PROFILE=${PROFILE} aws cloudformation describe-stacks \
  --stack-name aurora-cdc-msk-iam \
  --region ${REGION} \
  --query 'Stacks[0].Outputs[?OutputKey==`MSKConnectPluginBucketName`].OutputValue' \
  --output text)

if [ -z "$BUCKET_NAME" ]; then
  echo "Error: Could not retrieve S3 bucket name from CloudFormation stack"
  exit 1
fi

echo "Target S3 Bucket: ${BUCKET_NAME}"
echo ""

# Create temporary directory
WORK_DIR=$(mktemp -d)
echo "Working directory: ${WORK_DIR}"
cd ${WORK_DIR}

# Download Debezium PostgreSQL connector
echo "Downloading Debezium PostgreSQL connector..."
curl -L -o debezium-connector-postgres.tar.gz \
  "https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/${DEBEZIUM_VERSION}/debezium-connector-postgres-${DEBEZIUM_VERSION}-plugin.tar.gz"

# Download AWS Secrets Manager Config Provider
echo "Downloading AWS Secrets Manager Config Provider..."
curl -L -o aws-secrets-manager-config-provider.jar \
  "https://github.com/aws-samples/msk-config-providers/releases/download/v${SECRETS_PROVIDER_VERSION}/aws-secrets-manager-config-provider-${SECRETS_PROVIDER_VERSION}-all.jar"

# Extract files
echo "Extracting files..."
tar -xzf debezium-connector-postgres.tar.gz

# Create plugin structure
echo "Creating plugin package..."
mkdir -p ${PLUGIN_NAME}
cp -r debezium-connector-postgres/* ${PLUGIN_NAME}/
cp aws-secrets-manager-config-provider.jar ${PLUGIN_NAME}/

# Create ZIP for MSK Connect
echo "Creating ZIP archive..."
cd ${PLUGIN_NAME}
zip -r ../${PLUGIN_NAME}.zip .
cd ..

# Upload to S3
echo "Uploading to S3..."
AWS_PROFILE=${PROFILE} aws s3 cp ${PLUGIN_NAME}.zip s3://${BUCKET_NAME}/plugins/ --region ${REGION}

S3_URI="s3://${BUCKET_NAME}/plugins/${PLUGIN_NAME}.zip"
echo ""
echo "=== Upload Complete ==="
echo "S3 URI: ${S3_URI}"
echo ""
echo "Next step: Register custom plugin with MSK Connect"
echo ""
echo "AWS_PROFILE=${PROFILE} aws kafkaconnect create-custom-plugin \\"
echo "  --custom-plugin-name ${PLUGIN_NAME} \\"
echo "  --content-type ZIP \\"
echo "  --location s3Location={bucketArn=arn:aws:s3:::${BUCKET_NAME},fileKey=plugins/${PLUGIN_NAME}.zip} \\"
echo "  --region ${REGION}"

# Cleanup
cd /
rm -rf ${WORK_DIR}
echo ""
echo "Cleanup complete."
