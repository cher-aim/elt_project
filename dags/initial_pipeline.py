from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryCreateEmptyDatasetOperator
)
from datetime import datetime

from include.file_management.file_utils import read_config
from include.schema_generator.json_schema_generator import generate_json_schemas
from include.bigquery_utils.bigquery_utils import (
    prepare_table_creation_configs,
    get_dataset_creation_configs
)
# ----------------- main config  --------------------------- #

config_path = "/usr/local/airflow/include/configs/init_config.yaml"
config = read_config(config_path)

# ------------------ schema config -----------------------------# 

bronze_schema_config = read_config(config["paths"]["bronze_schema_config"])
silver_schema_config = read_config(config["paths"]["silver_schema_config"])

# ------------------ create empty dataset kwargs --------------- #

dataset_creation_config = get_dataset_creation_configs(
    datasets=config["datasets"],
    project_id=config["gcp"]["project_id"],
    gcp_conn_id=config["gcp"]["gcp_conn_id"]
) 

# ----------------- create empty table kwargs ----------------- #

bronze_table_creation_config = prepare_table_creation_configs(bronze_schema_config, config)
silver_table_creation_config = prepare_table_creation_configs(silver_schema_config, config)

# ------------------ dag arguments --------------------------- #

with DAG(
    dag_id=config['dag_args']['dag_id'],
    default_args=config['dag_args']['default_args'],
    schedule_interval=config['dag_args']['schedule_interval'],
    catchup=config['dag_args']['catchup'],
    start_date=datetime.combine(config['dag_args']['start_date'], datetime.min.time()),
    tags=["setup", "schema", "bigquery"]
) as dag:

# ------------------ tasks ------------------------------------- #
    generate_bronze_json_schema = PythonOperator(
        task_id="generate_bronze_schema_file",
        python_callable=generate_json_schemas,
        op_kwargs={
            "schema_config": bronze_schema_config,
            "output_folder": config["paths"]["bronze_local_schema_folder"]
        }
    )

    generate_silver_json_schema = PythonOperator(
        task_id="generate_silver_schema_file",
        python_callable=generate_json_schemas,
        op_kwargs={
            "schema_config": silver_schema_config,
            "output_folder": config["paths"]["silver_local_schema_folder"]
        }
    )

    upload_bronze_schema_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_bronze_schema_files_to_gcs",
        src=config["paths"]["bronze_local_schema_folder"] + "/*.json",
        dst=config["gcp"]["bronze_gcs_dst"],
        bucket=config["gcp"]["gcs_bucket"],
        gcp_conn_id=config["gcp"]["gcp_conn_id"],
        mime_type="application/json"
    )
    
    upload_silver_schema_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_silver_schema_files_to_gcs",
        src=config["paths"]["silver_local_schema_folder"] + "/*.json",
        dst=config["gcp"]["silver_gcs_dst"],
        bucket=config["gcp"]["gcs_bucket"],
        gcp_conn_id=config["gcp"]["gcp_conn_id"],
        mime_type="application/json"
    )
    
    create_empty_dataset = BigQueryCreateEmptyDatasetOperator.partial(
        task_id="create_dataset",
    ).expand_kwargs(dataset_creation_config)
    
    create_bronze_empty_table = BigQueryCreateEmptyTableOperator.partial(
        task_id="create_bronze_empty_table"
    ).expand_kwargs(bronze_table_creation_config)
    
    create_silver_empty_table = BigQueryCreateEmptyTableOperator.partial(
        task_id="create_silver_empty_table"
    ).expand_kwargs(silver_table_creation_config)
    
    generate_bronze_json_schema >> generate_silver_json_schema >> upload_bronze_schema_to_gcs >> upload_silver_schema_to_gcs
    upload_silver_schema_to_gcs >> create_empty_dataset >> create_bronze_empty_table >> create_silver_empty_table
