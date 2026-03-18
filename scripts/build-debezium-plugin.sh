#!/bin/bash
set -e

# Configuration — update these for your environment
DEBEZIUM_VERSION="2.7.3.Final"
PLUGIN_NAME="debezium-postgres-connector"
REGION="${AWS_REGION:-us-east-1}"
BUCKET_NAME="${1:?Usage: $0 <s3-bucket-name>}"

echo "=== Debezium PostgreSQL Connector Plugin Builder ==="
echo "Debezium Version: ${DEBEZIUM_VERSION}"
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

# Extract files
echo "Extracting files..."
tar -xzf debezium-connector-postgres.tar.gz

# Create plugin structure
echo "Creating plugin package..."
mkdir -p ${PLUGIN_NAME}
cp -r debezium-connector-postgres/* ${PLUGIN_NAME}/

# Create ZIP for MSK Connect
echo "Creating ZIP archive..."
cd ${PLUGIN_NAME}
zip -r ../${PLUGIN_NAME}.zip .
cd ..

# Upload to S3
echo "Uploading to S3..."
aws s3 cp ${PLUGIN_NAME}.zip s3://${BUCKET_NAME}/plugins/ --region ${REGION}

S3_URI="s3://${BUCKET_NAME}/plugins/${PLUGIN_NAME}.zip"
echo ""
echo "=== Upload Complete ==="
echo "S3 URI: ${S3_URI}"
echo ""
echo "Next step: Register custom plugin with MSK Connect"
echo ""
echo "aws kafkaconnect create-custom-plugin \\"
echo "  --custom-plugin-name ${PLUGIN_NAME} \\"
echo "  --content-type ZIP \\"
echo "  --location s3Location={bucketArn=arn:aws:s3:::${BUCKET_NAME},fileKey=plugins/${PLUGIN_NAME}.zip} \\"
echo "  --region ${REGION}"

# Cleanup
cd /
rm -rf ${WORK_DIR}
echo ""
echo "Cleanup complete."
