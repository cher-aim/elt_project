INSERT INTO `{{ params.project_id }}.reconcile.reconciliation_log`
(table_name, local_rows, bq_rows, match_status, load_date, dag_run_id)
VALUES
{% for row in ti.xcom_pull(task_ids='reconcile_source_to_bigquery') %}
    ("{{ row.table_name }}", {{ row.local_rows }}, {{ row.bq_rows }},
     "{{ row.match_status }}", "{{ row.load_date }}", "{{ row.dag_run_id }}")
    {% if not loop.last %},{% endif %}
{% endfor %}
