#!/usr/bin/env bash
# Triggera un DAG localmente via Airflow CLI.
# Uso: ./trigger_dag.sh <dag_id>
set -euo pipefail

if [[ $# -lt 1 ]]; then
    echo "Usage: $0 <dag_id>" >&2
    exit 1
fi

DAG_ID="$1"
RUN_ID="manual__$(date -u +%Y-%m-%dT%H:%M:%SZ)"

airflow dags trigger --run-id "$RUN_ID" "$DAG_ID"
