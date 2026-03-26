#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${LANYING_CONNECTOR_BASE_URL:-http://127.0.0.1:5000}"
API_KEY="${LANYING_CONNECTOR_API_KEY:-}"
MODEL="${LANYING_CONNECTOR_MODEL:-gpt-4o-mini}"
CASES_FILE="${LANYING_CONNECTOR_GRAY_CASES:-scripts/chat_replay_cases.json}"
REPORT_DIR="${LANYING_CONNECTOR_GRAY_REPORT_DIR:-scripts/reports}"
TIMESTAMP="$(date +%Y%m%d_%H%M%S)"
REPORT_FILE="${REPORT_DIR}/gray_validation_${TIMESTAMP}.log"

if [[ -z "${API_KEY}" ]]; then
  echo "error: missing LANYING_CONNECTOR_API_KEY" >&2
  echo "hint: export LANYING_CONNECTOR_API_KEY='YOUR_BEARER_TOKEN'" >&2
  exit 2
fi

if [[ ! -f "${CASES_FILE}" ]]; then
  echo "error: cases file not found: ${CASES_FILE}" >&2
  exit 2
fi

mkdir -p "${REPORT_DIR}"

echo "[gray] base_url=${BASE_URL}"
echo "[gray] model=${MODEL}"
echo "[gray] cases=${CASES_FILE}"
echo "[gray] report=${REPORT_FILE}"

set +e
python3 scripts/replay_chat_completions.py \
  --base-url "${BASE_URL}" \
  --api-key "${API_KEY}" \
  --model "${MODEL}" \
  --cases "${CASES_FILE}" | tee "${REPORT_FILE}"
EXIT_CODE=${PIPESTATUS[0]}
set -e

if [[ ${EXIT_CODE} -eq 0 ]]; then
  echo "[gray] PASS: all selected cases passed"
else
  echo "[gray] FAIL: some cases failed, check ${REPORT_FILE}" >&2
fi

exit ${EXIT_CODE}
