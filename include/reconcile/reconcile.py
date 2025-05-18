import os 
from collections import defaultdict
from tabulate import tabulate
from datetime import datetime
from zoneinfo import ZoneInfo
import pandas as pd
# from airflow.models import DagRun
from airflow.utils.context import Context
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def get_local_row_counts(parquet_folder_path: str) -> dict:
    """
    Count number of rows from local Parquet files for all source tables.

    Args:
        parquet_folder_path (str): Path to the folder containing source .parquet files

    Returns:
        dict: {table_name: row_count, ...}
    """
    row_counts = defaultdict(int)
    if not os.path.exists(parquet_folder_path):
        return FileNotFoundError(f"❌ path not found: {parquet_folder_path}")
    
    for file in os.listdir(parquet_folder_path):
        if file.endswith(".parquet"):
            file_path = os.path.join(parquet_folder_path, file)
            table_name, _ = os.path.splitext(file)

            try:
                df = pd.read_parquet(file_path)
                row_counts[table_name] += len(df)
            except Exception as e:
                print(f"⚠️ failed to read {file_path}: {e}")
                row_counts[table_name] = None
                
    return dict(row_counts)

def get_bigquery_row_counts(project_id, dataset, tables, gcp_conn_id):
    bq_hook = BigQueryHook(gcp_conn_id=gcp_conn_id)
    client = bq_hook.get_client(project_id=project_id)
    
    bq_counts = {}
    for table in tables:
        table_id = f"{project_id}.{dataset}.{table}"
        query = f"SELECT COUNT(*) AS count FROM `{table_id}`"
        result = client.query(query).result()
        bq_counts[table] = next(result)["count"] #extract only the first row of the result

    return bq_counts

def compare_counts(local_counts, bq_counts, **context: Context):
    dag_run_id = context["dag_run"].run_id
    # Convert UTC logical_date to Thailand time
    utc_dt = context["logical_date"]
    th_dt = utc_dt.astimezone(ZoneInfo("Asia/Bangkok"))  
    th_timestamp = th_dt.strftime("%Y-%m-%d %H:%M:%S")
    
    rows = []
    all_tables = sorted(set(local_counts) | set(bq_counts)) #set union (|) to ensure all tables are extracted from both local and bq
    
    for table in all_tables:
        local = local_counts.get(table, 0)
        bq = bq_counts.get(table, 0)
        match = "Match" if local == bq else "Mismatch"
        
        rows.append({
            "table_name": table, 
            "local_rows": local,
            "bq_rows": bq,
            "match_status": match,
            "load_date": th_timestamp,
            "dag_run_id": dag_run_id
        })
        
    print("\nReconciliation Summary:\n")
    print(tabulate(
        [[r["table_name"], r["local_rows"], r["bq_rows"], r["match_status"], r["load_date"]] for r in rows],
        headers=["Table", "Local", "BQ", "Status", "Load Time (TH)"],
        tablefmt="grid"
    ))
    
    return rows 