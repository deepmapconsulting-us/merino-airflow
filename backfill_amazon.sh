#!/usr/bin/env bash
# Trigger historical Amazon sales-traffic, orders, and ads backfills in Airflow.
set -euo pipefail

usage() {
  cat <<'EOF'
Usage: ./backfill_amazon.sh [options]

Options:
  --start-date YYYY-MM-DD   First date to import (default: 90 days through yesterday)
  --end-date YYYY-MM-DD     Last date to import (default: yesterday)
  --marketplaces LIST       Comma-separated: US,CA,MX,BR,AU
  --namespace NAME          Kubernetes namespace (default: airflow)
  --dry-run                 Print triggers without calling Airflow
  -h, --help                Show this help

This triggers amazon_sales_traffic, amazon_orders, and amazon_ads. Inventory
jobs are excluded because Amazon only exposes their current state.
EOF
}

read -r DEFAULT_START DEFAULT_END < <(
  python3 - <<'PY'
from datetime import date, timedelta

end = date.today() - timedelta(days=1)
print((end - timedelta(days=89)).isoformat(), end.isoformat())
PY
)

START_DATE="${DEFAULT_START}"
END_DATE="${DEFAULT_END}"
MARKETPLACES_RAW="US,CA,MX,BR,AU"
NAMESPACE="${NAMESPACE:-airflow}"
DRY_RUN=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --start-date)
      START_DATE="${2:?--start-date requires YYYY-MM-DD}"
      shift 2
      ;;
    --end-date)
      END_DATE="${2:?--end-date requires YYYY-MM-DD}"
      shift 2
      ;;
    --marketplaces)
      MARKETPLACES_RAW="${2:?--marketplaces requires a comma-separated list}"
      shift 2
      ;;
    --namespace)
      NAMESPACE="${2:?--namespace requires a value}"
      shift 2
      ;;
    --dry-run)
      DRY_RUN=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

VALIDATED="$(
  python3 - "${START_DATE}" "${END_DATE}" "${MARKETPLACES_RAW}" <<'PY'
import json
import sys
from datetime import date

try:
    start = date.fromisoformat(sys.argv[1])
    end = date.fromisoformat(sys.argv[2])
except ValueError as error:
    raise SystemExit(f"Dates must use YYYY-MM-DD: {error}")

if end < start:
    raise SystemExit(f"End date {end} is before start date {start}")

allowed = {"US", "CA", "MX", "BR", "AU"}
marketplaces = []
for raw in sys.argv[3].split(","):
    marketplace = raw.strip().upper()
    if not marketplace:
        continue
    if marketplace not in allowed:
        raise SystemExit(f"Unsupported marketplace: {marketplace}")
    if marketplace not in marketplaces:
        marketplaces.append(marketplace)

if not marketplaces:
    raise SystemExit("At least one marketplace is required")

print(json.dumps({
    "start": start.isoformat(),
    "end": end.isoformat(),
    "marketplaces": marketplaces,
    "overwrite": True,
}))
PY
)"

echo "Amazon backfill: ${START_DATE} -> ${END_DATE}"
echo "Configuration: ${VALIDATED}"
echo

POD=""
if [[ "${DRY_RUN}" -eq 0 ]]; then
  POD="$(
    kubectl -n "${NAMESPACE}" get pods \
      -l component=scheduler \
      --field-selector status.phase=Running \
      -o jsonpath='{.items[0].metadata.name}'
  )"
  if [[ -z "${POD}" ]]; then
    echo "No running scheduler pod found in namespace ${NAMESPACE}" >&2
    exit 1
  fi
fi

timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
for dag_id in amazon_sales_traffic amazon_orders amazon_ads; do
  run_id="manual__${START_DATE}__${END_DATE}__${timestamp}"
  if [[ "${DRY_RUN}" -eq 1 ]]; then
    echo "DRY RUN ${dag_id} --run-id ${run_id} --conf ${VALIDATED}"
    continue
  fi

  echo "Triggering ${dag_id} (${run_id})"
  kubectl -n "${NAMESPACE}" exec "${POD}" -c scheduler -- \
    airflow dags trigger "${dag_id}" \
    --run-id "${run_id}" \
    --conf "${VALIDATED}"
done

if [[ "${DRY_RUN}" -eq 0 ]]; then
  echo
  echo "Triggered all historical Amazon backfills."
  echo "SP-API jobs will serialize through the amazon_sp_api pool."
fi
