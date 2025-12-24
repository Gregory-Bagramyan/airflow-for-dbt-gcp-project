from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime

with DAG(
    dag_id="dbt_simple_cloud_run",
    start_date=datetime(2025, 12, 19),
    schedule_interval="0 2 * * *",  # 02:00 UTC
    catchup=False,
    default_args={"retries": 0},
    tags=["dbt", "cloud-run"],
) as dag:

    run_dbt = CloudRunExecuteJobOperator(
        task_id="run_dbt",
        project_id="ancient-folio-481614-p3",
        region="europe-west1",
        job_name="dbt-build-job",
    )
