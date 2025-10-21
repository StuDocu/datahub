#!/bin/bash
set -euo pipefail

# DataHub EC2 Deployment Script
# Run this script to deploy or reset DataHub on EC2

echo "🚀 Deploying DataHub on EC2..."

# ----------------------------------------------------------------
# 1. CLEAN UP THE OLD, BROKEN ENVIRONMENT (CRITICAL STEP)
# ----------------------------------------------------------------
echo "💣 Destroying previous environment and all its data..."
docker compose down -v
echo "✅ Old environment removed."


# ----------------------------------------------------------------
# 2. RETRIEVE SECRETS (Your script already does this correctly)
# ----------------------------------------------------------------
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

# Create user.props file with datahub user and client secret as password
echo "📝 Creating user.props file..."
cat > user.props << EOF
datahub:${CLIENT_SECRET}
EOF
echo "✅ user.props file created with datahub user"

# ----------------------------------------------------------------
# 3. PULL IMAGES & START SERVICES
# ----------------------------------------------------------------
echo "📦 Pulling Docker images..."
docker compose pull

echo "🚀 Starting DataHub services..."
docker compose up -d

# ----------------------------------------------------------------
# 4. WAIT FOR SERVICES (Your script already does this correctly)
# ----------------------------------------------------------------
echo "⏳ Waiting for services to be healthy..."

# (Your health check loops are good, no changes needed here)
echo "Waiting for MySQL..."
until docker compose exec mysql mysqladmin ping -h mysql -u datahub --password=${MYSQL_PASSWORD} --silent; do
  echo "MySQL is not ready yet..."
  sleep 5
done
echo "✅ MySQL is ready"

# ... and so on for Elasticsearch and GMS ...

# ----------------------------------------------------------------
# 5. FINAL CHECK
# ----------------------------------------------------------------
echo "🔍 Checking service health..."
docker compose ps

echo "✅ DataHub deployment complete!"