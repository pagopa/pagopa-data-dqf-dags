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
# Watermark args opzionali (None = non passati al job Spark).
WATERMARK_COLUMN = None
WATERMARK_FROM = None
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

XREF_DATASETS = {}
DATASET_PK_MAP = {}


def _spark_overrides(entity: str, contract_path: str) -> dict:
    args = [
        f"--domain={SYSTEM}",
        f"--contract-path={contract_path}",
        f"--repository={REPOSITORY}",
        f"--ref={REF}",
        f"--dag-id={DAG_ID}",
        f"--airflow-run-id={DAG_ID}_{{{{ ts_nodash }}}}",
    ]
    
    if XREF_DATASETS and entity in XREF_DATASETS:
        xrefs = XREF_DATASETS[entity]
        xrefs_str = ",".join(xrefs) if isinstance(xrefs, list) else xrefs
        args.append(f"--xref-datasets={xrefs_str}")
        
    if DATASET_PK_MAP and entity in DATASET_PK_MAP:
        pks = DATASET_PK_MAP[entity]
        pks_str = ",".join(pks) if isinstance(pks, list) else pks
        args.append(f"--primary-keys={pks_str}")
        
    if WATERMARK_COLUMN:
        args.append(f"--watermark-column={WATERMARK_COLUMN}")
    if WATERMARK_FROM:
        args.append(f"--watermark-from={WATERMARK_FROM}")
    return {
        "spark": {
            "args": args,
            # Override runtime delle env var lette dal framework via os.getenv.
            # Sovrascrive quanto baked-in nel CDE Spark job al cde job create.
            "conf": {
                "spark.kubernetes.driverEnv.ENV": ENV,
                "spark.executorEnv.ENV": ENV,
                "spark.kubernetes.driverEnv.SYSTEM": SYSTEM,
                "spark.executorEnv.SYSTEM": SYSTEM,
            },
        }
    }


def _log_runtime_env(**context):
    import json
    config_dump = {
        "repository": REPOSITORY,
        "ref": REF,
        "schedule": SCHEDULE,
        "email": EMAIL,
        "contracts": CONTRACTS,
        "xref_datasets": XREF_DATASETS,
        "dataset_pk_map": DATASET_PK_MAP,
    }
    logging.info("SYSTEM=%s ENV=%s REF=%s JOB_NAME=%s SCHEDULE=%s", SYSTEM, ENV, REF, JOB_NAME, SCHEDULE)
    logging.info("dag_run.run_id=%s", context["dag_run"].run_id)
    logging.info("CONFIG JSON:\n%s", json.dumps(config_dump, indent=2))


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

    #Esecuzione in parallelo
    for entity, contract_path in CONTRACTS.items():
        dq_task = CdeRunJobOperator(
            task_id=f"dq_{entity}",
            retries=1,
            job_name=JOB_NAME,
            overrides=_spark_overrides(entity, contract_path),
        )
        log_env >> dq_task
