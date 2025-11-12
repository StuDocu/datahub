#!/bin/bash
set -euo pipefail
echo "🚀 Deploying DataHub on EC2..."


docker compose down

echo "🔐 Retrieving secrets from AWS Secrets Manager..."
# Get DataHub client secret
CLIENT_SECRET=$(aws secretsmanager get-secret-value --secret-id "datahub/client-secret" --query SecretString --output text)
# Get OIDC credentials
OIDC_CREDS=$(aws secretsmanager get-secret-value --secret-id "datahub/oidc-credentials" --query SecretString --output text)
AUTH_OIDC_CLIENT_ID=$(echo "$OIDC_CREDS" | jq -r '.AUTH_OIDC_CLIENT_ID')
AUTH_OIDC_CLIENT_SECRET=$(echo "$OIDC_CREDS" | jq -r '.AUTH_OIDC_CLIENT_SECRET')
# Get database credentials
DB_CREDS=$(aws secretsmanager get-secret-value --secret-id "datahub/database-credentials" --query SecretString --output text)
MYSQL_PASSWORD=$(echo "$DB_CREDS" | jq -r '.MYSQL_PASSWORD')
MYSQL_ROOT_PASSWORD=$(echo "$DB_CREDS" | jq -r '.MYSQL_ROOT_PASSWORD')
echo "✅ Secrets retrieved and environment variables set."


ENV_FILE="/home/ubuntu/datahub/.env"
echo "💾 Writing environment variables to $ENV_FILE..."
cat > "$ENV_FILE" <<EOF
CLIENT_SECRET=$CLIENT_SECRET
AUTH_OIDC_CLIENT_ID=$AUTH_OIDC_CLIENT_ID
AUTH_OIDC_CLIENT_SECRET=$AUTH_OIDC_CLIENT_SECRET
MYSQL_PASSWORD=$MYSQL_PASSWORD
MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD
EOF


echo "📝 Creating user.props file for datahub root user account..."
cat > user.props <<EOF
datahub:${CLIENT_SECRET}
EOF
echo "✅ user.props file created with datahub user."


echo "📦 Pulling Docker images..."
docker compose pull --quiet

echo "🚀 Starting DataHub services..."
set -a
source "$ENV_FILE"
set +a
docker compose --env-file /home/ubuntu/datahub/.env up -d


echo "⏳ Waiting for services to be healthy..."

echo "Waiting for MySQL..."
until docker compose exec mysql mysqladmin ping -h mysql -u datahub --password="${MYSQL_PASSWORD}" --silent; do
  echo "MySQL is not ready yet..."
  sleep 5
done
echo "✅ MySQL is ready."


echo "✅ DataHub deployment complete!"
