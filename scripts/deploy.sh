#!/usr/bin/env bash
# Deploy del DAG DQ pagoPA su CDE per una coppia (system, env).
#
# Uso: ./scripts/deploy.sh <system> <env> [--create]
#   <system>  es. gpd, gec, ...
#   <env>     es. test, prod
#
# Convenzioni:
#   - template:     dag/pagopa_dq.py            (unico per tutti i system/env)
#   - config:       dags_config/<system>/<env>.json
#   - deployed:     dag_dq_<system>_quality_<env>.py
#   - CDE job:      dag_dq_<system>_quality_<env>
set -euo pipefail

SYSTEM="${1:-}"
ENV="${2:-}"
CREATE="${3:-}"

if [[ -z "$SYSTEM" || -z "$ENV" ]]; then
    echo "Usage: $0 <system> <env> [--create]" >&2
    echo "Esempio: $0 gpd test --create" >&2
    exit 1
fi

TEMPLATE="dag/pagopa_dq.py"
CONFIG="dags_config/${SYSTEM}/${ENV}.json"
DIST_DIR="dist"
JOB_NAME="dag_dq_${SYSTEM}_quality_${ENV}"
DEPLOYED_NAME="${JOB_NAME}.py"
RESOURCE="pagopa-dqf-dags"

if [[ ! -f "$TEMPLATE" ]]; then
    echo "Template non trovato: $TEMPLATE" >&2
    exit 1
fi
if [[ ! -f "$CONFIG" ]]; then
    echo "Config non trovata: $CONFIG" >&2
    exit 1
fi

python3 scripts/render.py "$SYSTEM" "$ENV" "$TEMPLATE" "$CONFIG" "${DIST_DIR}/${DEPLOYED_NAME}"

cde resource upload --name "$RESOURCE" \
    --local-path "${DIST_DIR}/${DEPLOYED_NAME}" \
    --resource-path "$DEPLOYED_NAME"
echo "Uploaded $DEPLOYED_NAME to resource $RESOURCE"

if [[ "$CREATE" == "--create" ]]; then
    cde job create --name "$JOB_NAME" --type airflow \
        --dag-file "$DEPLOYED_NAME" \
        --mount-1-resource "$RESOURCE" \
        --schedule-paused false
    echo "Created job $JOB_NAME"
fi
