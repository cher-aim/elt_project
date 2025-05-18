from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime 
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from include.file_management.file_utils import read_config, convert_to_parquet
from include.bigquery_utils.bigquery_utils import get_ingestion_task_configs

config = read_config("/usr/local/airflow/include/configs/bronze_config.yaml")
schema_config = read_config(config['paths']['schema_config'])
ingestion_task_config = get_ingestion_task_configs(
    schema_config, 
    config 
)

with DAG(
    dag_id=config['dag_args']['dag_id'],
    default_args=config['dag_args']['default_args'],
    schedule_interval=config['dag_args']['schedule_interval'],
    catchup=config['dag_args']['catchup'],
    start_date=datetime.combine(config['dag_args']['start_date'], datetime.min.time()),
    tags=["setup", "schema", "bigquery"]
) as dag:
    
    convert_csv_source_to_parquet = PythonOperator(
        task_id="convert_csv_source_to_parquet",
        python_callable=convert_to_parquet,
        op_kwargs={
            "input_csv_folder_path": config["paths"]["local_source_data"],
            "output_parquet_folder_path": config["paths"]["local_parquet_source_data"]
        }
    )
    
    upload_source_data_task = LocalFilesystemToGCSOperator(
        task_id="upload_source_files_to_gcs",
        src=config['paths']['local_parquet_source_data'] + "/*.parquet",
        dst=config['gcp']['gcs_dst'],
        bucket=config['gcp']['gcs_bucket'],
        gcp_conn_id=config['gcp']['gcp_conn_id'],
        mime_type="application/octet-stream"
    )
    
    load_bronze_data = GCSToBigQueryOperator.partial(
        task_id="load_bronze_data"
    ).expand_kwargs(ingestion_task_config)
    
    convert_csv_source_to_parquet >> upload_source_data_task >> load_bronze_data