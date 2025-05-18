from airflow import DAG
from airflow.models.xcom_arg import XComArg
from airflow.operators.python import PythonOperator
from datetime import datetime 
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

from include.file_management.file_utils import read_config, read_sql
from include.reconcile.reconcile import get_bigquery_row_counts, get_local_row_counts, compare_counts

config = read_config("/usr/local/airflow/include/configs/reconcile_config.yaml")
schema_config = read_config(config['paths']['bronze_schema_config'])

with DAG(
    dag_id="reconcile_pipeline",
    default_args=config['dag_args']['default_args'],
    schedule_interval=config['dag_args']['schedule_interval'],
    catchup=config['dag_args']['catchup'],
    start_date=datetime.combine(config['dag_args']['start_date'], datetime.min.time()),
    tags=["setup", "schema", "bigquery"]
) as dag:
    
        create_reconcile_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id="create_reconcile_dataset",
            dataset_id="reconcile",
            project_id=config["gcp"]["project_id"],
            gcp_conn_id=config["gcp"]["gcp_conn_id"],
            exists_ok=True,  
            location="US"   
        )
        
        create_reconciliation_log_table = BigQueryInsertJobOperator(
        task_id="create_reconciliation_log_table",
        configuration={
            "query": {
                "query": read_sql(config["paths"]["sql_scripts"]["create_reconciliation_log_table"]),
                "useLegacySql": False
            }
        },
        params={
            "project_id": config["gcp"]["project_id"]
            },
        gcp_conn_id=config["gcp"]["gcp_conn_id"]
        )
        
        get_local_counts_task = PythonOperator(
            task_id="get_local_counts",
            python_callable=get_local_row_counts,
            op_kwargs={"parquet_folder_path": config["paths"]["local_parquet_source_data"]}
        )

        get_bq_counts_task = PythonOperator(
            task_id="get_bq_counts",
            python_callable=get_bigquery_row_counts,
            op_kwargs={
                "project_id": config["gcp"]["project_id"],
                "dataset": "bronze",
                "tables": XComArg(get_local_counts_task),  # ต้อง extract .keys() ในฟังก์ชันเอง
                "gcp_conn_id": config["gcp"]["gcp_conn_id"]
            }
        )

        reconcile_task = PythonOperator(
            task_id="reconcile_source_to_bigquery",
            python_callable=compare_counts,
            op_kwargs={
                "local_counts": XComArg(get_local_counts_task),
                "bq_counts": XComArg(get_bq_counts_task),
            }
        )

        insert_reconcilation_log = BigQueryInsertJobOperator(
            task_id="write_reconciliation_result",
            configuration={
            "query": {
                "query": read_sql(config["paths"]["sql_scripts"]["insert_reconciliation_log"]),
                "useLegacySql": False
            }
            },
            params={
            "project_id": config["gcp"]["project_id"]
            },
            gcp_conn_id=config["gcp"]["gcp_conn_id"]
        )
    
        
        create_reconcile_dataset >> create_reconciliation_log_table >> get_local_counts_task >> get_bq_counts_task >> reconcile_task >> insert_reconcilation_log