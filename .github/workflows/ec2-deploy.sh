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

# Metabase credentials — secret key is the login email, value is the password
METABASE_CREDS=$(aws secretsmanager get-secret-value --region "$AWS_REGION" \
  --secret-id "metabase/user_data" --query SecretString --output text)
METABASE_USERNAME=$(echo "$METABASE_CREDS" | jq -r 'keys[0]')
METABASE_PASSWORD=$(echo "$METABASE_CREDS" | jq -r '.[keys[0]]')

# Redshift credentials for the datahub_user
REDSHIFT_CREDS=$(aws secretsmanager get-secret-value --region "$AWS_REGION" \
  --secret-id "Redshift/production-cluster/datahub_user" --query SecretString --output text)
REDSHIFT_USERNAME=$(echo "$REDSHIFT_CREDS" | jq -r '.username')
REDSHIFT_PASSWORD=$(echo "$REDSHIFT_CREDS" | jq -r '.password')

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
LOGIN_PAYLOAD=$(jq -n --arg password "$CLIENT_SECRET" '{"username":"datahub","password":$password}')
GMS_TOKEN=$(curl -sf -X POST http://localhost:8080/logIn \
  -H "Content-Type: application/json" \
  -d "$LOGIN_PAYLOAD" | jq -r '.accessToken')

SECRET_UPSERT_FAILED=0

upsert_datahub_secret() {
  local name="$1" value="$2"
  # Use jq to build the payload so that quotes, backslashes, and newlines in
  # the value are properly JSON-escaped — never interpolate secrets into raw strings.
  local payload
  payload=$(jq -n \
    --arg name "$name" \
    --arg value "$value" \
    '{"query":"mutation($name:String!,$value:String!){upsertSecret(input:{name:$name,value:$value}){urn}}","variables":{"name":$name,"value":$value}}')
  local response
  response=$(curl -sf -X POST http://localhost:8080/api/graphql \
    -H "Authorization: Bearer $GMS_TOKEN" \
    -H "Content-Type: application/json" \
    -d "$payload")
  if [ $? -ne 0 ]; then
    echo "ERROR: Failed to upsert DataHub secret '$name' (HTTP error)" >&2
    SECRET_UPSERT_FAILED=1
  elif echo "$response" | jq -e '.errors' > /dev/null 2>&1; then
    echo "ERROR: GraphQL error upserting secret '$name': $(echo "$response" | jq -r '.errors[0].message')" >&2
    SECRET_UPSERT_FAILED=1
  else
    echo "✅ Secret upserted: $name"
  fi
}

upsert_datahub_secret "METABASE_USERNAME" "$METABASE_USERNAME"
upsert_datahub_secret "METABASE_PASSWORD" "$METABASE_PASSWORD"
upsert_datahub_secret "REDSHIFT_USERNAME" "$REDSHIFT_USERNAME"
upsert_datahub_secret "REDSHIFT_PASSWORD" "$REDSHIFT_PASSWORD"

if [ "$SECRET_UPSERT_FAILED" -ne 0 ]; then
  echo "ERROR: one or more DataHub secrets failed to upsert; aborting deploy." >&2
  echo "Recipes that reference \${METABASE_*} or \${REDSHIFT_*} would run with unresolved credentials." >&2
  exit 1
fi

echo "Waiting for datahub-actions container to be ready..."
until docker exec datahub-datahub-actions-1 datahub version > /dev/null 2>&1; do
  echo "datahub-actions not ready yet, retrying..."
  sleep 5
done
echo "datahub-actions is ready."

echo "Deploying ingestion recipes to DataHub UI..."
RECIPE_DEPLOY_FAILED=0

deploy_recipe() {
  local name="$1" schedule="$2" tz="$3" recipe="$4"
  if docker exec datahub-datahub-actions-1 \
    datahub ingest deploy \
      --name "$name" \
      --schedule "$schedule" \
      --time-zone "$tz" \
      -c "$recipe"; then
    echo "✅ Deployed: $name"
  else
    echo "ERROR: Failed to deploy recipe '$name'" >&2
    RECIPE_DEPLOY_FAILED=1
  fi
}

deploy_recipe "DBT"                 "0 0 * * *"  "Europe/Amsterdam" /ingestion/recipes/dbt.yml
deploy_recipe "Glue"                "0 3 * * *"  "Europe/Amsterdam" /ingestion/recipes/glue.yml
deploy_recipe "Metabase"            "0 8 * * *"  "Europe/Amsterdam" /ingestion/recipes/metabase.yml
deploy_recipe "Redshift Production" "0 21 * * *" "Europe/Amsterdam" /ingestion/recipes/redshift_production.yml

if [ "$RECIPE_DEPLOY_FAILED" -ne 0 ]; then
  echo "ERROR: one or more ingestion recipes failed to deploy." >&2
  exit 1
fi

echo "DataHub deployment complete."
