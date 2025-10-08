#!/bin/bash
set -euo pipefail

# DataHub EC2 Deployment Script
# Run this script to deploy DataHub on EC2

echo "🚀 Deploying DataHub on EC2..."

# Get secrets from AWS Secrets Manager and export them
echo "🔐 Retrieving secrets from AWS Secrets Manager..."

# Get DataHub client secret
export CLIENT_SECRET=$(aws secretsmanager get-secret-value --secret-id "datahub/client-secret" --query SecretString --output text)

# Get OIDC credentials
OIDC_CREDS=$(aws secretsmanager get-secret-value --secret-id "datahub/oidc-credentials" --query SecretString --output text)
export AUTH_OIDC_CLIENT_ID=$(echo $OIDC_CREDS | jq -r '.AUTH_OIDC_CLIENT_ID')
export AUTH_OIDC_CLIENT_SECRET=$(echo $OIDC_CREDS | jq -r '.AUTH_OIDC_CLIENT_SECRET')

# Get database credentials
DB_CREDS=$(aws secretsmanager get-secret-value --secret-id "datahub/database-credentials" --query SecretString --output text)
export MYSQL_PASSWORD=$(echo $DB_CREDS | jq -r '.MYSQL_PASSWORD')
export MYSQL_ROOT_PASSWORD=$(echo $DB_CREDS | jq -r '.MYSQL_ROOT_PASSWORD')

echo "✅ Secrets retrieved and environment variables set"


# Pull latest images
echo "📦 Pulling Docker images..."
docker compose pull

# Start services
echo "🚀 Starting DataHub services..."
docker-compose up -d

# Wait for services to be healthy
echo "⏳ Waiting for services to be healthy..."
sleep 30

# Check service health
echo "🔍 Checking service health..."
docker-compose ps

# Display access information
echo "✅ DataHub deployment complete!"
