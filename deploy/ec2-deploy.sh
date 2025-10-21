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


# Pull latest images (optional - can be slow)
echo "📦 Pulling Docker images..."
docker compose pull

# Start services with environment variables
echo "🚀 Starting DataHub services..."
MYSQL_PASSWORD=${MYSQL_PASSWORD} \
MYSQL_ROOT_PASSWORD=${MYSQL_ROOT_PASSWORD} \
CLIENT_SECRET=${CLIENT_SECRET} \
AUTH_OIDC_CLIENT_ID=${AUTH_OIDC_CLIENT_ID} \
AUTH_OIDC_CLIENT_SECRET=${AUTH_OIDC_CLIENT_SECRET} \
docker compose up -d

# Wait for services to be healthy with proper health checks
echo "⏳ Waiting for services to be healthy..."

# Wait for MySQL to be ready
echo "Waiting for MySQL..."
until MYSQL_PASSWORD=${MYSQL_PASSWORD} docker compose exec mysql mysqladmin ping -h mysql -u datahub --password=${MYSQL_PASSWORD} --silent; do
  echo "MySQL is not ready yet..."
  sleep 5
done
echo "✅ MySQL is ready"

# Wait for Elasticsearch to be ready
echo "Waiting for Elasticsearch..."
until curl -s http://localhost:9200/_cluster/health > /dev/null; do
  echo "Elasticsearch is not ready yet..."
  sleep 5
done
echo "✅ Elasticsearch is ready"

# Wait for DataHub GMS to be ready
echo "Waiting for DataHub GMS..."
until curl -s http://localhost:8080/health > /dev/null; do
  echo "DataHub GMS is not ready yet..."
  sleep 5
done
echo "✅ DataHub GMS is ready"

# Check service health
echo "🔍 Checking service health..."
docker compose ps

# Display access information
echo "✅ DataHub deployment complete!"
