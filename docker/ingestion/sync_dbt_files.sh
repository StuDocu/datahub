#!/bin/bash
set -euo pipefail

# --- Configuration Variables ---
TODAYS_DATE=$(date +%Y-%m-%d)
S3_BUCKET="s3://production.dbt-data-artefacts/dbt_datalake_source_main_pipelines_dbt_datalake_source_models/${TODAYS_DATE}/target/"
# Local directory where artifacts will be synced
LOCAL_DIR="dbt_artifacts"

# --- Create Local Directory if It Does Not Exist ---
if [ ! -d "$LOCAL_DIR" ]; then
  echo "Local directory $LOCAL_DIR does not exist. Creating it..."
  mkdir -p "$LOCAL_DIR"
fi

# --- Sync the dbt Artifacts ---
echo "Starting sync of dbt artifacts from $S3_BUCKET to $LOCAL_DIR ..."
aws s3 sync "$S3_BUCKET" "$LOCAL_DIR"
echo "Sync complete. dbt artifacts are now available in $LOCAL_DIR."

CATALOG_PARQUET="$LOCAL_DIR/catalog.json"
MANIFEST_PARQUET="$LOCAL_DIR/manifest.json"

# --- Spin up Container using docker-compose ---
echo "Spinning up container using dbt-ingestion-docker-compose.yml..."
docker-compose -f dbt-ingestion-docker-compose.yml up -d
echo "Container has been spun up successfully."
