#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${LANYING_CONNECTOR_BASE_URL:-http://127.0.0.1:5000}"
API_KEY="${LANYING_CONNECTOR_API_KEY:-}"
MODEL="${LANYING_CONNECTOR_MODEL:-gpt-4o-mini}"
MODELS="${LANYING_CONNECTOR_MODELS:-}"
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
echo "[gray] cases=${CASES_FILE}"
echo "[gray] report=${REPORT_FILE}"

if [[ -n "${MODELS}" ]]; then
  IFS=',' read -r -a MODEL_LIST <<< "${MODELS}"
else
  MODEL_LIST=("${MODEL}")
fi

EXIT_CODE=0
PASS_MODELS=0
FAIL_MODELS=0
TOTAL_MODELS=0

{
  echo "[gray] model_count=${#MODEL_LIST[@]}"
} | tee "${REPORT_FILE}" >/dev/null

for RAW_MODEL in "${MODEL_LIST[@]}"; do
  NOW_MODEL="$(echo "${RAW_MODEL}" | xargs)"
  if [[ -z "${NOW_MODEL}" ]]; then
    continue
  fi
  TOTAL_MODELS=$((TOTAL_MODELS + 1))
  SAFE_MODEL="$(echo "${NOW_MODEL}" | tr '/: ' '___')"
  MODEL_REPORT_FILE="${REPORT_DIR}/gray_validation_${TIMESTAMP}_${SAFE_MODEL}.log"

  echo ""
  echo "========== MODEL: ${NOW_MODEL} =========="
  {
    echo ""
    echo "========== MODEL: ${NOW_MODEL} =========="
    echo "[gray] model_report=${MODEL_REPORT_FILE}"
  } | tee -a "${REPORT_FILE}" >/dev/null

  set +e
  python3 scripts/replay_chat_completions.py \
    --base-url "${BASE_URL}" \
    --api-key "${API_KEY}" \
    --model "${NOW_MODEL}" \
    --cases "${CASES_FILE}" | tee "${MODEL_REPORT_FILE}"
  MODEL_EXIT_CODE=${PIPESTATUS[0]}
  set -e

  cat "${MODEL_REPORT_FILE}" >> "${REPORT_FILE}"

  if [[ ${MODEL_EXIT_CODE} -eq 0 ]]; then
    PASS_MODELS=$((PASS_MODELS + 1))
    echo "[gray] MODEL PASS: ${NOW_MODEL}"
    echo "[gray] MODEL PASS: ${NOW_MODEL}" >> "${REPORT_FILE}"
  else
    FAIL_MODELS=$((FAIL_MODELS + 1))
    EXIT_CODE=1
    echo "[gray] MODEL FAIL: ${NOW_MODEL} (check ${MODEL_REPORT_FILE})" >&2
    echo "[gray] MODEL FAIL: ${NOW_MODEL} (check ${MODEL_REPORT_FILE})" >> "${REPORT_FILE}"
  fi
done

echo ""
echo "[gray] MODEL SUMMARY: pass=${PASS_MODELS}, fail=${FAIL_MODELS}, total=${TOTAL_MODELS}"
{
  echo ""
  echo "[gray] MODEL SUMMARY: pass=${PASS_MODELS}, fail=${FAIL_MODELS}, total=${TOTAL_MODELS}"
} >> "${REPORT_FILE}"

if [[ ${EXIT_CODE} -eq 0 ]]; then
  echo "[gray] PASS: all selected models/cases passed"
else
  echo "[gray] FAIL: some models/cases failed, check ${REPORT_FILE}" >&2
fi

exit ${EXIT_CODE}
