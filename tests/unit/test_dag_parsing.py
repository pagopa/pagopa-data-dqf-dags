"""Parse check via Airflow DagBag.

Per ogni (system, env) in dags_config/, renderizza il template in dist/ e parsa con DagBag.
Verifica: nessun errore di import, almeno un DAG, dag_id unici.

Note:
- Pulisce dist/ prima di renderizzare per non parsare artefatti vecchi.
- Setta una connection dummy `cde_runtime_api` via env var perché il CdeRunJobOperator di
  Cloudera fa get_connection() a parse-time. In locale la connection non esiste; la dummy
  evita l'AirflowNotFoundException senza affettare la struttura del DAG.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

# Deve venire PRIMA dell'import di airflow per essere letta da Connection.
os.environ.setdefault("AIRFLOW_CONN_CDE_RUNTIME_API", "http://dummy")

import pytest  # noqa: E402
from airflow.models import DagBag  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[2]
TEMPLATE = REPO_ROOT / "dag" / "pagopa_dq.py"
CONFIGS_ROOT = REPO_ROOT / "dags_config"
DIST_DIR = REPO_ROOT / "dist"

sys.path.insert(0, str(REPO_ROOT / "scripts"))
from render import render  # noqa: E402


@pytest.fixture(scope="module")
def rendered_dist() -> Path:
    # Pulisci dist/ dai .py vecchi prima di rigenerare.
    DIST_DIR.mkdir(exist_ok=True)
    for f in DIST_DIR.glob("*.py"):
        f.unlink()

    for sys_dir in CONFIGS_ROOT.iterdir():
        if not sys_dir.is_dir():
            continue
        system = sys_dir.name
        for cfg in sys_dir.glob("*.json"):
            env = cfg.stem
            out = DIST_DIR / f"dag_dq_{system}_quality_{env}.py"
            render(system, env, TEMPLATE, cfg, out)
    return DIST_DIR


@pytest.fixture(scope="module")
def dag_bag(rendered_dist: Path) -> DagBag:
    return DagBag(dag_folder=str(rendered_dist), include_examples=False)


def test_no_import_errors(dag_bag: DagBag) -> None:
    assert not dag_bag.import_errors, f"Errori di import nei DAG: {dag_bag.import_errors}"


def test_at_least_one_dag(dag_bag: DagBag) -> None:
    assert dag_bag.dags, "Nessun DAG renderizzato/parsato"


def test_dag_ids_unique(dag_bag: DagBag) -> None:
    ids = list(dag_bag.dags.keys())
    assert len(ids) == len(set(ids)), f"dag_id duplicati: {ids}"
