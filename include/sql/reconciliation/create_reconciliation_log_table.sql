CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.reconcile.reconciliation_log` (
  table_name STRING,
  local_rows INT64,
  bq_rows INT64,
  match_status STRING,
  load_date DATETIME,
  dag_run_id STRING
)
PARTITION BY DATE(load_date);