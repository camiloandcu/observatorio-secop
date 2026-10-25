"""Manual bounded analytics; operators retain fixed inputs through every retry."""

from datetime import timedelta

import pendulum
from airflow.sdk import DAG, task

from observatorio_secop.orchestration.models import PipelineError, execution_id
from observatorio_secop.orchestration.runner import run_stage

with DAG(
    dag_id="secop_pipeline",
    schedule=None,
    catchup=False,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    max_active_runs=1,
    max_active_tasks=1,
    dagrun_timeout=timedelta(hours=2),
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(seconds=120),
        "execution_timeout": timedelta(minutes=30),
        "pool": "secop_writer",
    },
    tags=["secop", "bounded", "analytics"],
) as dag:

    @task
    def execute_stage(stage: str) -> str:
        from airflow.exceptions import AirflowFailException
        from airflow.sdk import get_current_context

        context = get_current_context()
        run = context["dag_run"]
        identity = execution_id("secop_pipeline", run.run_id)
        try:
            run_stage(stage, identity, dict(run.conf) if stage == "validate" else None)
        except PipelineError as error:
            if error.code != "PIPELINE_TRANSIENT":
                raise AirflowFailException(error.code) from None
            raise RuntimeError(error.code) from None
        return identity  # small safe reference; durable receipt is the authority

    previous = None
    for stage in ("validate", "acquire", "silver", "warehouse", "candidate", "activate", "outcome"):
        current = execute_stage.override(task_id=stage)(stage)
        if previous is not None:
            previous >> current
        previous = current
