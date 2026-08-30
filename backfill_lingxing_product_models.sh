#!/usr/bin/env bash
# Trigger a catalog-only LingXing import to backfill erp_logistics.product.model.
set -euo pipefail

NAMESPACE="${NAMESPACE:-airflow}"
DRY_RUN=0

usage() {
  cat <<'EOF'
Usage: ./backfill_lingxing_product_models.sh [options]

Options:
  --namespace NAME  Kubernetes namespace (default: airflow)
  --dry-run         Print the Airflow trigger without executing it
  -h, --help        Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
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

DAG_ID="lingxing_erp_logistics_import"
CONF='{"products_only":true,"skip_order_profit":true}'
RUN_ID="manual__lingxing_product_models__$(date -u +%Y%m%dT%H%M%SZ)"

if [[ "${DRY_RUN}" -eq 1 ]]; then
  echo "DRY RUN ${DAG_ID} --run-id ${RUN_ID} --conf ${CONF}"
  exit 0
fi

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

echo "Triggering LingXing product model backfill (${RUN_ID})"
kubectl -n "${NAMESPACE}" exec "${POD}" -c scheduler -- \
  airflow dags trigger "${DAG_ID}" \
  --run-id "${RUN_ID}" \
  --conf "${CONF}"

echo
echo "LingXing product model backfill queued in Airflow."
