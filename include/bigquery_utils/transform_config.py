from include.file_management.file_utils import read_sql
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import os
from include.file_management.file_utils import read_sql

def get_transformation_view_configs(config):
    base_sql_path = config['paths']['transformation_sql']
    gcp_conn_id = config['gcp']['gcp_conn_id']

    view_configs_by_tier = {}

    for tier in sorted(os.listdir(base_sql_path)):
        tier_path = os.path.join(base_sql_path, tier)
        if not os.path.isdir(tier_path):
            continue

        tier_configs = []
        for file in sorted(os.listdir(tier_path)):
            if file.endswith(".sql"):
                table_name = os.path.splitext(file)[0]  
                tier_configs.append({
                    "task_id": f"create_{table_name}_table",
                    "sql_path": os.path.join(tier_path, file),
                    "table_name": table_name,  
                    "gcp_conn_id": gcp_conn_id,
                    "transform_dataset": config['gcp']['dataset']
                })

        if tier_configs:
            view_configs_by_tier[tier] = tier_configs

    return view_configs_by_tier

def format_tier_for_expand(tiers_config, project_id):
    formatted = []
    for entry in tiers_config:
        sql = read_sql(entry["sql_path"])

        formatted.append({
            "task_id": entry["task_id"],
            "configuration": {
                "query": {
                    "query": sql,
                    "useLegacySql": False
                }
            },
            "params": {
                "project_id": project_id,
                "table_name": entry["table_name"],
                "transform_dataset": entry["transform_dataset"]
            },
            "gcp_conn_id": entry["gcp_conn_id"]
        })
    return formatted

