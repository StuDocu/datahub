#!/bin/bash
set -euo pipefail

echo "Deploying DataHub on EC2..."

# Pinned DataHub version. All acryldata/* images (gms, frontend, actions, upgrade,
# and the *-setup jobs) resolve from this. Override by exporting DATAHUB_VERSION.
DATAHUB_VERSION="${DATAHUB_VERSION:-v1.6.0}"
AWS_REGION="${AWS_REGION:-eu-west-1}"

DATAHUB_DIR="/home/ubuntu/datahub"
ENV_FILE="$DATAHUB_DIR/.env"
USER_PROPS_FILE="$DATAHUB_DIR/user.props"

cd "$DATAHUB_DIR"

echo "Retrieving secrets from AWS Secrets Manager..."

# DataHub client/system secret (also used as the datahub root user password)
CLIENT_SECRET=$(aws secretsmanager get-secret-value --region "$AWS_REGION" \
  --secret-id "datahub/client-secret" --query SecretString --output text)

# Google OIDC credentials
OIDC_CREDS=$(aws secretsmanager get-secret-value --region "$AWS_REGION" \
  --secret-id "datahub/oidc-credentials" --query SecretString --output text)
AUTH_OIDC_CLIENT_ID=$(echo "$OIDC_CREDS" | jq -r '.AUTH_OIDC_CLIENT_ID')
AUTH_OIDC_CLIENT_SECRET=$(echo "$OIDC_CREDS" | jq -r '.AUTH_OIDC_CLIENT_SECRET')

# MySQL credentials
DB_CREDS=$(aws secretsmanager get-secret-value --region "$AWS_REGION" \
  --secret-id "datahub/database-credentials" --query SecretString --output text)
MYSQL_PASSWORD=$(echo "$DB_CREDS" | jq -r '.MYSQL_PASSWORD')
MYSQL_ROOT_PASSWORD=$(echo "$DB_CREDS" | jq -r '.MYSQL_ROOT_PASSWORD')

# Token service signing material. Kept stable across deploys; rotating these
# invalidates existing DataHub access tokens.
TOKEN_CREDS=$(aws secretsmanager get-secret-value --region "$AWS_REGION" \
  --secret-id "datahub/token-service" --query SecretString --output text)
DATAHUB_TOKEN_SERVICE_SIGNING_KEY=$(echo "$TOKEN_CREDS" | jq -r '.DATAHUB_TOKEN_SERVICE_SIGNING_KEY')
DATAHUB_TOKEN_SERVICE_SALT=$(echo "$TOKEN_CREDS" | jq -r '.DATAHUB_TOKEN_SERVICE_SALT')

# Ingestion credentials (Metabase + Redshift) — used by recipes as DataHub Secrets.
# Secret format: {"METABASE_USERNAME":"...","METABASE_PASSWORD":"...","REDSHIFT_USERNAME":"...","REDSHIFT_PASSWORD":"..."}
INGESTION_CREDS=$(aws secretsmanager get-secret-value --region "$AWS_REGION" \
  --secret-id "datahub/ingestion-credentials" --query SecretString --output text)
METABASE_USERNAME=$(echo "$INGESTION_CREDS" | jq -r '.METABASE_USERNAME')
METABASE_PASSWORD=$(echo "$INGESTION_CREDS" | jq -r '.METABASE_PASSWORD')
REDSHIFT_USERNAME=$(echo "$INGESTION_CREDS" | jq -r '.REDSHIFT_USERNAME')
REDSHIFT_PASSWORD=$(echo "$INGESTION_CREDS" | jq -r '.REDSHIFT_PASSWORD')

echo "Secrets retrieved."

# Fail fast if any required value is empty, so we never deploy a half-configured stack.
for var in CLIENT_SECRET AUTH_OIDC_CLIENT_ID AUTH_OIDC_CLIENT_SECRET \
  MYSQL_PASSWORD MYSQL_ROOT_PASSWORD \
  DATAHUB_TOKEN_SERVICE_SIGNING_KEY DATAHUB_TOKEN_SERVICE_SALT DATAHUB_VERSION \
  METABASE_USERNAME METABASE_PASSWORD REDSHIFT_USERNAME REDSHIFT_PASSWORD; do
  if [ -z "${!var:-}" ] || [ "${!var}" = "null" ]; then
    echo "ERROR: required value '$var' is empty; aborting deploy." >&2
    exit 1
  fi
done

echo "Writing environment file to $ENV_FILE..."
# Create with restrictive permissions before writing any secret content.
install -m 600 /dev/null "$ENV_FILE"
cat > "$ENV_FILE" <<EOF
CLIENT_SECRET=$CLIENT_SECRET
AUTH_OIDC_CLIENT_ID=$AUTH_OIDC_CLIENT_ID
AUTH_OIDC_CLIENT_SECRET=$AUTH_OIDC_CLIENT_SECRET
MYSQL_PASSWORD=$MYSQL_PASSWORD
MYSQL_ROOT_PASSWORD=$MYSQL_ROOT_PASSWORD
DATAHUB_VERSION=$DATAHUB_VERSION
DATAHUB_TOKEN_SERVICE_SIGNING_KEY=$DATAHUB_TOKEN_SERVICE_SIGNING_KEY
DATAHUB_TOKEN_SERVICE_SALT=$DATAHUB_TOKEN_SERVICE_SALT
EOF
chmod 600 "$ENV_FILE"

echo "Creating user.props for the datahub root user account..."
install -m 600 /dev/null "$USER_PROPS_FILE"
cat > "$USER_PROPS_FILE" <<EOF
datahub:${CLIENT_SECRET}
EOF
chmod 600 "$USER_PROPS_FILE"
echo "user.props created."

echo "Pulling Docker images..."
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a
docker compose --env-file "$ENV_FILE" pull --quiet

echo "Starting DataHub services..."
docker compose --env-file "$ENV_FILE" up -d --remove-orphans

echo "Waiting for MySQL..."
until docker compose exec -T mysql mysqladmin ping -h mysql -u datahub --password="${MYSQL_PASSWORD}" --silent; do
  echo "MySQL is not ready yet..."
  sleep 5
done
echo "MySQL is ready."


echo "Waiting for DataHub GMS to be healthy..."
until curl -sf http://localhost:8080/health > /dev/null 2>&1; do
  echo "GMS not ready yet, retrying..."
  sleep 5
done
echo "DataHub GMS is healthy."

echo "Syncing ingestion credentials into DataHub secrets store..."
# DataHub recipes use ${SECRET_NAME} substitution resolved from the GMS secrets store
# (backed by MySQL). Re-upserting on every deploy ensures secrets survive volume resets
# and stay in sync with AWS Secrets Manager as the source of truth.
GMS_TOKEN=$(curl -sf -X POST http://localhost:8080/logIn \
  -H "Content-Type: application/json" \
  -d "{\"username\":\"datahub\",\"password\":\"${CLIENT_SECRET}\"}" | jq -r '.accessToken')

upsert_datahub_secret() {
  local name="$1" value="$2"
  # Use jq to build the payload so that quotes, backslashes, and newlines in
  # the value are properly JSON-escaped — never interpolate secrets into raw strings.
  local payload
  payload=$(jq -n \
    --arg name "$name" \
    --arg value "$value" \
    '{"query":"mutation($name:String!,$value:String!){upsertSecret(input:{name:$name,value:$value}){urn}}","variables":{"name":$name,"value":$value}}')
  curl -sf -X POST http://localhost:8080/api/graphql \
    -H "Authorization: Bearer $GMS_TOKEN" \
    -H "Content-Type: application/json" \
    -d "$payload" \
    > /dev/null \
  && echo "✅ Secret upserted: $name" \
  || echo "⚠️  Failed to upsert secret: $name"
}

upsert_datahub_secret "METABASE_USERNAME" "$METABASE_USERNAME"
upsert_datahub_secret "METABASE_PASSWORD" "$METABASE_PASSWORD"
upsert_datahub_secret "REDSHIFT_USERNAME" "$REDSHIFT_USERNAME"
upsert_datahub_secret "REDSHIFT_PASSWORD" "$REDSHIFT_PASSWORD"

echo "Deploying ingestion recipes to DataHub UI..."
deploy_recipe() {
  local name="$1" schedule="$2" tz="$3" recipe="$4"
  docker exec datahub-datahub-actions-1 \
    datahub ingest deploy \
      --name "$name" \
      --schedule "$schedule" \
      --time-zone "$tz" \
      -c "$recipe" \
  && echo "✅ Deployed: $name" \
  || echo "⚠️  Failed to deploy: $name (will retry on next deploy)"
}

deploy_recipe "DBT"                 "0 0 * * *"  "Europe/Amsterdam" /ingestion/recipes/dbt.yml
deploy_recipe "Glue"                "0 3 * * *"  "Europe/Amsterdam" /ingestion/recipes/glue.yml
deploy_recipe "Metabase"            "0 8 * * *"  "Europe/Amsterdam" /ingestion/recipes/metabase.yml
deploy_recipe "Redshift Production" "0 21 * * *" "Europe/Amsterdam" /ingestion/recipes/redshift_production.yml
echo "DataHub deployment complete."
