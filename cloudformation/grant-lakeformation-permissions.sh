#!/bin/bash

# Grant Lake Formation permissions to Firehose role for S3 Tables access

set -e

AWS_PROFILE="${1:-achintan-secondary}"
AWS_REGION="${2:-us-east-1}"
FIREHOSE_ROLE_ARN="${3}"

if [ -z "$FIREHOSE_ROLE_ARN" ]; then
    echo "Usage: $0 [aws-profile] [region] <firehose-role-arn>"
    echo "Example: $0 achintan-secondary us-east-1 arn:aws:iam::123456789012:role/FirehoseRole"
    exit 1
fi

echo "Granting Lake Formation permissions to: $FIREHOSE_ROLE_ARN"

# Grant permissions on database
aws lakeformation grant-permissions \
    --principal DataLakePrincipalIdentifier="$FIREHOSE_ROLE_ARN" \
    --resource '{"Database":{"Name":"aurora_cdc"}}' \
    --permissions "ALL" \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION"

echo "✅ Granted ALL permissions on database: aurora_cdc"

# Grant permissions on all tables in the database
aws lakeformation grant-permissions \
    --principal DataLakePrincipalIdentifier="$FIREHOSE_ROLE_ARN" \
    --resource '{"Table":{"DatabaseName":"aurora_cdc","TableWildcard":{}}}' \
    --permissions "ALL" \
    --profile "$AWS_PROFILE" \
    --region "$AWS_REGION"

echo "✅ Granted ALL permissions on all tables in aurora_cdc database"
echo ""
echo "Lake Formation permissions granted successfully!"
