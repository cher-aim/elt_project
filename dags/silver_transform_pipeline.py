from airflow import DAG
from airflow.utils.task_group import TaskGroup
from datetime import datetime 
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from include.file_management.file_utils import read_config
from include.bigquery_utils.transform_config import (
    get_transformation_view_configs,
    format_tier_for_expand
)
from include.bigquery_utils.bigquery_utils import (
    prepare_dynamic_insert_configs,
    drop_bigquery_tables_by_prefixes
)

# --------------- Read configs ---------------------------------- #
config = read_config("/usr/local/airflow/include/configs/silver_config.yaml")
schema_config = read_config(config["paths"]["schema_config"])
# constraints_config = config["paths"]["constraints_folder"]
# -------------- get config of each teir task ------------------- #
tiers_config = get_transformation_view_configs(config)

# --------------- tier configs ----------------------------------- #
t1_config = format_tier_for_expand(tiers_config["t1"], config["gcp"]["project_id"])
t2_config = format_tier_for_expand(tiers_config["t2"], config["gcp"]["project_id"])
t3_config = format_tier_for_expand(tiers_config["t3"], config["gcp"]["project_id"])
t4_config = format_tier_for_expand(tiers_config["t4"], config["gcp"]["project_id"])

# --------------- prepare dynamic insert config --------------------- #
insert_task_config = prepare_dynamic_insert_configs(schema_config, config["gcp"]["project_id"], config["gcp"]["gcp_conn_id"])

# ----------------- DAG ------------------------------------ #
with DAG(
    dag_id=config["dag_args"]["dag_transform_id"],
    default_args=config["dag_args"]["default_args"],
    schedule_interval=config["dag_args"]["schedule_interval"],
    catchup=config["dag_args"]["catchup"],
    start_date=datetime.combine(config["dag_args"]["start_date"], datetime.min.time()),
    tags=["transformation", "silver"]
) as dag:
    
    create_t1_tables = BigQueryInsertJobOperator.partial(
        task_id="create_t1_tables"
    ).expand_kwargs(t1_config)
    
    create_t2_tables = BigQueryInsertJobOperator.partial(
        task_id="create_t2_tables"
    ).expand_kwargs(t2_config)
    
    create_t3_tables = BigQueryInsertJobOperator.partial(
        task_id="create_t3_tables"
    ).expand_kwargs(t3_config)
    
    create_t4_tables = BigQueryInsertJobOperator.partial(
        task_id="create_t4_tables"
    ).expand_kwargs(t4_config)   

    with TaskGroup(group_id="insert_data_taskgroup") as insert_data_taskgroup:
        insert_data = BigQueryInsertJobOperator.partial(
            task_id="insert_data_task"
    ).expand_kwargs(insert_task_config)
    
    drop_temp_tables = PythonOperator(
        task_id="drop_all_silver_t1_to_t4_tables",
        python_callable=drop_bigquery_tables_by_prefixes,
        op_kwargs={
            "gcp_conn_id": config["gcp"]["gcp_conn_id"],
            "project_id": config["gcp"]["project_id"],
            "dataset": config["gcp"]["dataset"],
            "prefixes": ["t1_", "t2_", "t3_", "t4_"]
        }
    )
   
    
    create_t1_tables >> create_t2_tables >> create_t3_tables >> create_t4_tables >> insert_data_taskgroup >> drop_temp_tables