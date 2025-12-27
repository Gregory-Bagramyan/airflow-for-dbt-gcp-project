from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook
from datetime import datetime
import json


def slack_fail_alert(context):
    """
    Slack notification on task failure.
    Uses HttpHook + BaseHook.
    """

    ti = context["task_instance"]

    # Safety: avoid Slack spam if retries are ever enabled
    if ti.try_number < ti.max_tries:
        return

    # Build message
    message = (
        ":red_circle: *Airflow task failed*\n"
        f"*DAG*: `{ti.dag_id}`\n"
        f"*Task*: `{ti.task_id}`\n"
        f"*Execution*: `{context['execution_date']}`\n"
        f"*Logs*: <{ti.log_url}|View logs>"
    )

    # Fetch Slack connection properly
    conn = BaseHook.get_connection("slack_webhook")
    webhook_token = conn.extra_dejson["webhook_token"]

    # Send Slack message via HTTP hook
    http = HttpHook(
        method="POST",
        http_conn_id="slack_webhook",
    )

    http.run(
        endpoint=webhook_token,
        headers={"Content-Type": "application/json"},
        data=json.dumps({"text": message}),
    )


with DAG(
    dag_id="dbt_simple_cloud_run",
    start_date=datetime(2025, 12, 19),
    schedule_interval="0 2 * * *",  # 02:00 UTC
    catchup=False,
    default_args={
        "retries": 0,
        "on_failure_callback": slack_fail_alert,
    },
    tags=["dbt", "cloud-run"],
) as dag:

    run_dbt = CloudRunExecuteJobOperator(
        task_id="run_dbt",
        project_id="ancient-folio-481614-p3",
        region="europe-west1",
        job_name="dbt-build-job",
    )
