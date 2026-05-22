"""Pagopa DQ Dag - template unico, rendered per (system, env) da scripts/render.py."""

import logging

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from cloudera.airflow.providers.operators.cde import CdeRunJobOperator

_log = logging.getLogger(__name__)

# === Config (default dev/gpd; sostituite al deploy da scripts/render.py + dags_config/<system>/<env>.json) ===
SYSTEM = "gpd"
ENV = "dev"
REPOSITORY = "carlomanco-qty/qty-data-contracts"
REF = "main"
SCHEDULE = None
CONTRACTS = {
    "payment_option": "src/data/pagopa/gpd/silver/dc-gpd-payment_option.yaml",
    "payment_position": "src/data/pagopa/gpd/silver/dc-gpd-payment_position.yaml",
    "transfer": "src/data/pagopa/gpd/silver/dc-gpd-transfer.yaml",
}
EMAIL = ["carlo.manco@quantyca.it"]
# === End config ===

JOB_NAME = f"dq-quality-{ENV}"
DAG_ID = f"dag_dq_{SYSTEM}_quality_{ENV}"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": EMAIL,
}

_log.info("[%s] parse-time SYSTEM=%s ENV=%s REF=%s JOB_NAME=%s", DAG_ID, SYSTEM, ENV, REF, JOB_NAME)


def _spark_overrides(contract_path: str) -> dict:
    return {
        "spark": {
            "args": [
                f"--contract-path={contract_path}",
                f"--repository={REPOSITORY}",
                f"--ref={REF}",
                f"--env={ENV}",
                f"--system={SYSTEM}",
            ],
        }
    }


def _log_runtime_env(**context):
    logging.info("SYSTEM=%s ENV=%s REF=%s JOB_NAME=%s SCHEDULE=%s", SYSTEM, ENV, REF, JOB_NAME, SCHEDULE)
    logging.info("dag_run.run_id=%s", context["dag_run"].run_id)


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.datetime(2026, 5, 22, tz="UTC"),
    schedule=SCHEDULE,
    catchup=False,
    render_template_as_native_obj=True,
    tags=["pagopa", SYSTEM, "dq", ENV],
) as dag:

    log_env = PythonOperator(
        task_id="log_env",
        python_callable=_log_runtime_env,
    )

    for entity, contract_path in CONTRACTS.items():
        dq_task = CdeRunJobOperator(
            task_id=f"dq_{entity}",
            retries=1,
            job_name=JOB_NAME,
            overrides=_spark_overrides(contract_path),
        )
        log_env >> dq_task
