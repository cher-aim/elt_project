from typing import List
from google.cloud import bigquery
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def get_dataset_creation_configs(datasets: list, project_id: str, gcp_conn_id: str) -> list:
    """
    Prepare list of kwargs for expanding BigQueryCreateEmptyDatasetOperator.
    """
    return [
        {
            "dataset_id": dataset["name"],
            "project_id": project_id,
            "gcp_conn_id": gcp_conn_id,
            "location": dataset.get("location", "US")  # default to US if not provided
        }
        for dataset in datasets
    ]

def prepare_table_creation_configs(schema_config, config):
    """
    Prepare list of kwargs for expanding a create empty table task.
    """
    configs = []
    
    for table in schema_config["tables"]:
        table_resource = {
            "tableReference": {
                "projectId": config["gcp"]["project_id"],
                "datasetId": table["dataset"],
                "tableId": table["table_name"],
            },
            "schema": {
                "fields": table["schema"],
            }
        }
        
        # Optional partitioning support
        if "partitioning" in table:
            part_type = table["partitioning"].get("type", "").upper()
            if part_type == "INGESTION":
                table_resource["timePartitioning"] = {"type": "DAY"}
            elif part_type == "TIME":
                table_resource["timePartitioning"] = {
                    "type": "DAY",
                    "field": table["partitioning"]["column"]
                }

        # Optional clustering support
        if "clustering" in table:
            table_resource["clustering"] = {
                "fields": table["clustering"]
            }

        configs.append({
            "project_id": config["gcp"]["project_id"],
            "dataset_id": table["dataset"],
            "table_id": table["table_name"],
            "table_resource": table_resource,
            "gcp_conn_id": config["gcp"]["gcp_conn_id"]
        })

    return configs



def get_ingestion_task_configs(schema_config, config):
    task_configs = []
    bucket = config['gcp']['gcs_bucket']
    project_id = config['gcp']['project_id']
    gcp_conn_id = config['gcp']['gcp_conn_id']

    for table in schema_config["tables"]:
        table_name = table["table_name"]
        dataset = table["dataset"]

        task_configs.append({
            "task_id": f"load_{table_name}_to_bronze",
            "bucket": bucket,
            "source_objects": [f"{config['gcp']['gcs_dst']}{table_name}.parquet"],
            "destination_project_dataset_table": f"{project_id}.{dataset}.{table_name}",
            "source_format": "PARQUET",
            "write_disposition": "WRITE_TRUNCATE",
            "schema_fields":table["schema"],
            "gcp_conn_id": gcp_conn_id
        })

    return task_configs

def prepare_dynamic_insert_configs(
    schema_config: dict,
    project_id: str,
    gcp_conn_id: str
):
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    client = bq_hook.get_client(project_id=project_id)

    insert_configs = []

    for table in schema_config.get("tables", []):
        table_name = table["table_name"]
        dataset = table["dataset"]

        columns = [col["name"] for col in table.get("schema", [])]
        column_list_str = ", ".join(columns)
        
        source_table = f"{project_id}.{dataset}.t4_{table_name}"
        target_table = f"{project_id}.{dataset}.{table_name}"

        sql = f"""
        INSERT INTO `{target_table}` ({column_list_str})
        SELECT {column_list_str}
        FROM `{source_table}`
        """

        insert_configs.append({
            "configuration": {
                "query": {
                    "query": sql,
                    "useLegacySql": False
                }
            },
            "gcp_conn_id": gcp_conn_id
        })

    return insert_configs

def drop_bigquery_tables_by_prefixes(
    gcp_conn_id: str,
    project_id: str,
    dataset: str,
    prefixes: List[str],
    ignore_if_missing: bool = True
) -> None:
    """
    Drops all BigQuery tables in a dataset that match any of the specified prefixes.

    Args:
        gcp_conn_id (str): Airflow GCP connection ID
        project_id (str): GCP project ID
        dataset (str): BigQuery dataset name
        prefixes (List[str]): List of table name prefixes to match (e.g. ["t1_", "t2_"])
        ignore_if_missing (bool): If True, ignore missing tables
    """
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    client = bq_hook.get_client(project_id=project_id)
    dataset_ref = bigquery.DatasetReference(project_id, dataset)

    tables = client.list_tables(dataset_ref)
    tables_to_drop = [
        table.table_id
        for table in tables
        if any(table.table_id.startswith(prefix) for prefix in prefixes)
    ]

    print(f"🔍 Found {len(tables_to_drop)} tables to drop: {tables_to_drop}")

    # Drop matched tables
    for table_id in tables_to_drop:
        full_table_id = f"{project_id}.{dataset}.{table_id}"
        try:
            client.delete_table(full_table_id, not_found_ok=ignore_if_missing)
            print(f"✅ Dropped table: {full_table_id}")
        except Exception as e:
            print(f"❌ Failed to drop table {full_table_id}: {e}")