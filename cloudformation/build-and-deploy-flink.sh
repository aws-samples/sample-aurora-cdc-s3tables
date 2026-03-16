#!/bin/bash

# Build and deploy Flink application for Aurora CDC to S3 Tables

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
FLINK_DIR="$PROJECT_DIR/flink"

# Configuration
S3_BUCKET="${1:-msk-connect-plugins-063337766236-us-east-1}"
AWS_REGION="${2:-us-east-1}"
AWS_PROFILE="${3:-achintan-secondary}"

echo "Building Flink application..."
cd "$FLINK_DIR"

# Build with Maven
if ! command -v mvn &> /dev/null; then
    echo "Error: Maven is not installed. Please install Maven first."
    exit 1
fi

mvn clean package

# Upload JAR to S3
JAR_FILE="target/aurora-cdc-flink-1.0.0.jar"
if [ ! -f "$JAR_FILE" ]; then
    echo "Error: JAR file not found at $JAR_FILE"
    exit 1
fi

echo "Uploading JAR to S3..."
aws s3 cp "$JAR_FILE" "s3://$S3_BUCKET/flink/" \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION"

echo "✅ Flink application JAR uploaded to s3://$S3_BUCKET/flink/aurora-cdc-flink-1.0.0.jar"
echo ""
echo "Next steps:"
echo "1. Get S3 Tables bucket ARN"
echo "2. Deploy CloudFormation stack:"
echo ""
echo "aws cloudformation create-stack \\"
echo "  --stack-name aurora-cdc-flink \\"
echo "  --template-body file://$PROJECT_DIR/cloudformation/flink-cdc-s3tables.yaml \\"
echo "  --parameters \\"
echo "    ParameterKey=FlinkApplicationJarS3Bucket,ParameterValue=$S3_BUCKET \\"
echo "    ParameterKey=KafkaBootstrapServers,ParameterValue=<MSK_BOOTSTRAP_SERVERS> \\"
echo "    ParameterKey=S3TablesBucketArn,ParameterValue=<S3_TABLES_BUCKET_ARN> \\"
echo "    ParameterKey=SubnetIds,ParameterValue=<SUBNET_IDS> \\"
echo "    ParameterKey=SecurityGroupIds,ParameterValue=<SECURITY_GROUP_IDS> \\"
echo "  --capabilities CAPABILITY_IAM \\"
echo "  --profile $AWS_PROFILE \\"
echo "  --region $AWS_REGION"
