#!/usr/bin/env python3
"""
DMS Lineage Emitter for DataHub.

Dynamically discovers AWS DMS replication tasks and emits lineage:
  Aurora/RDS source tables -> DMS DataJob -> Glue catalog tables

Fully automatic: new DMS tasks and new tables are picked up on each run
without any manual changes to this script.

Schedule: runs daily after the Glue ingestion (which catalogs the S3 output)
so that Glue tables already exist in DataHub when lineage edges are emitted.
"""
import json
import logging

import boto3
from datahub.emitter.mce_builder import (
    make_data_flow_urn,
    make_data_job_urn,
    make_dataset_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DATAHUB_GMS_URL = "http://localhost:8080"
AWS_REGION = "eu-west-1"
ENV = "PROD"

ENGINE_TO_PLATFORM = {
    "aurora": "mysql",
    "mysql": "mysql",
    "postgres": "postgres",
    "aurora-postgresql": "postgres",
}


def get_all_dms_tasks(dms_client: object) -> list:
    tasks = []
    kwargs: dict = {"WithoutSettings": False}
    while True:
        resp = dms_client.describe_replication_tasks(**kwargs)
        tasks.extend(resp["ReplicationTasks"])
        marker = resp.get("Marker")
        if not marker:
            break
        kwargs["Marker"] = marker
    return tasks


def get_endpoint(dms_client: object, arn: str) -> dict:
    resp = dms_client.describe_endpoints(
        Filters=[{"Name": "endpoint-arn", "Values": [arn]}]
    )
    return resp["Endpoints"][0] if resp["Endpoints"] else {}


def find_glue_tables_for_s3_prefix(
    glue_client: object, bucket: str, folder: str
) -> list:
    """Return (database, table) pairs whose S3 location starts with s3://bucket/folder/."""
    # Trailing slash prevents folder2 from matching when looking for folder
    s3_prefix = f"s3://{bucket}/{folder}".rstrip("/") + "/"
    matches = []
    db_paginator = glue_client.get_paginator("get_databases")
    for db_page in db_paginator.paginate():
        for db in db_page["DatabaseList"]:
            db_name = db["Name"]
            table_paginator = glue_client.get_paginator("get_tables")
            for table_page in table_paginator.paginate(DatabaseName=db_name):
                for table in table_page["TableList"]:
                    location = (
                        table.get("StorageDescriptor", {}).get("Location", "")
                    )
                    if location.startswith(s3_prefix):
                        matches.append((db_name, table["Name"]))
    return matches


def parse_explicit_source_tables(table_mappings_json: str) -> list:
    """
    Extract (schema, table) pairs from DMS selection rules.
    Skips wildcard rules (%) — those are resolved via Glue table discovery.
    """
    results = []
    try:
        rules = json.loads(table_mappings_json).get("rules", [])
        for rule in rules:
            if (
                rule.get("rule-type") == "selection"
                and rule.get("rule-action") == "include"
            ):
                locator = rule.get("object-locator", {})
                schema = locator.get("schema-name", "%")
                table = locator.get("table-name", "%")
                if "%" not in schema and "%" not in table:
                    results.append((schema, table))
    except Exception as exc:
        logger.warning("Could not parse table mappings: %s", exc)
    return results


def emit_lineage() -> None:
    dms = boto3.client("dms", region_name=AWS_REGION)
    glue = boto3.client("glue", region_name=AWS_REGION)
    emitter = DatahubRestEmitter(DATAHUB_GMS_URL)

    tasks = get_all_dms_tasks(dms)
    logger.info("Discovered %d DMS tasks", len(tasks))

    for task in tasks:
        task_id = task["ReplicationTaskIdentifier"]
        instance_id = task["ReplicationInstanceArn"].split(":")[-1]

        source_ep = get_endpoint(dms, task["SourceEndpointArn"])
        target_ep = get_endpoint(dms, task["TargetEndpointArn"])

        if target_ep.get("EngineName", "").lower() != "s3":
            logger.info(
                "Skipping %s — target is not S3 (%s)",
                task_id,
                target_ep.get("EngineName"),
            )
            continue

        s3 = target_ep.get("S3Settings", {})
        bucket = s3.get("BucketName", "")
        folder = s3.get("BucketFolder", "")
        if not bucket:
            logger.warning("Skipping %s — no S3 bucket in target endpoint", task_id)
            continue

        glue_tables = find_glue_tables_for_s3_prefix(glue, bucket, folder)
        if not glue_tables:
            logger.warning(
                "No Glue tables found for %s at s3://%s/%s", task_id, bucket, folder
            )
            continue

        platform = ENGINE_TO_PLATFORM.get(
            source_ep.get("EngineName", "").lower(), "mysql"
        )
        source_db = source_ep.get("DatabaseName", "")
        explicit_tables = parse_explicit_source_tables(task.get("TableMappings", "{}"))

        # Upstream: Aurora/RDS source tables (explicit names only; wildcards resolved via Glue)
        input_urns = []
        for schema, table in explicit_tables:
            name = f"{source_db}.{schema}.{table}" if source_db else f"{schema}.{table}"
            input_urns.append(make_dataset_urn(platform, name, ENV))

        # Downstream: Glue catalog tables (already ingested by the Glue recipe)
        output_urns = [
            make_dataset_urn("glue", f"{db}.{tbl}", ENV)
            for db, tbl in glue_tables
        ]

        flow_urn = make_data_flow_urn("dms", instance_id, ENV)
        job_urn = make_data_job_urn("dms", instance_id, task_id, ENV)

        emitter.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=flow_urn,
                aspect=DataFlowInfoClass(
                    name=instance_id,
                    customProperties={"platform": "AWS DMS"},
                ),
            )
        )
        emitter.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInfoClass(
                    name=task_id,
                    type="BATCH",
                    customProperties={
                        "migrationType": task.get("MigrationType", ""),
                        "status": task.get("Status", ""),
                        "replicationInstance": instance_id,
                    },
                ),
            )
        )
        emitter.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=input_urns,
                    outputDatasets=output_urns,
                ),
            )
        )

        logger.info(
            "%s: %d upstream Aurora tables -> %d downstream Glue tables",
            task_id,
            len(input_urns),
            len(output_urns),
        )

    logger.info("DMS lineage emission complete")


if __name__ == "__main__":
    emit_lineage()
