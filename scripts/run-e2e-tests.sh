#!/usr/bin/env bash
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPORT_DIR="${ROOT_DIR}/target/e2e-test-report"
REPORT_FILE="${REPORT_DIR}/summary.md"

INCLUDE_DOCKER=false
DOCKER_MODE=full
INCLUDE_SOAK=false
STRICT=false
SOAK_MINUTES=10
SOAK_OPS=100

usage() {
  cat <<'EOF'
Usage: scripts/run-e2e-tests.sh [options]

Runs NGrid/NQueue/NMap e2e gates, continues after failures, and writes a
pass-rate report to target/e2e-test-report/summary.md.

Options:
  --docker              Include the Docker/Testcontainers resilience gate.
  --docker-split        Run Docker map/queue subsets instead of the full Docker gate.
  --soak                Include a short soak test.
  --all                 Include Docker/Testcontainers and soak gates.
  --strict              Exit non-zero if any executed gate fails.
  --soak-minutes <n>    Soak duration in minutes (default: 10).
  --soak-ops <n>        Soak target ops/sec (default: 100).
  -h, --help            Show this help.

Default gates:
  - focused core queue/map checks
  - in-process resilience profile

Examples:
  scripts/run-e2e-tests.sh
  scripts/run-e2e-tests.sh --docker
  scripts/run-e2e-tests.sh --docker-split
  scripts/run-e2e-tests.sh --all --soak-minutes 30 --strict
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --docker)
      INCLUDE_DOCKER=true
      DOCKER_MODE=full
      shift
      ;;
    --docker-split)
      INCLUDE_DOCKER=true
      DOCKER_MODE=split
      shift
      ;;
    --soak)
      INCLUDE_SOAK=true
      shift
      ;;
    --all)
      INCLUDE_DOCKER=true
      INCLUDE_SOAK=true
      shift
      ;;
    --strict)
      STRICT=true
      shift
      ;;
    --soak-minutes)
      SOAK_MINUTES="${2:-}"
      shift 2
      ;;
    --soak-ops)
      SOAK_OPS="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage
      exit 2
      ;;
  esac
done

if ! [[ "${SOAK_MINUTES}" =~ ^[0-9]+$ ]] || [[ "${SOAK_MINUTES}" -lt 1 ]]; then
  echo "--soak-minutes must be a positive integer" >&2
  exit 2
fi

if ! [[ "${SOAK_OPS}" =~ ^[0-9]+$ ]] || [[ "${SOAK_OPS}" -lt 1 ]]; then
  echo "--soak-ops must be a positive integer" >&2
  exit 2
fi

mkdir -p "${REPORT_DIR}"
rm -f "${REPORT_DIR}"/*.log "${REPORT_FILE}"

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0
TOTAL_COUNT=0
RESULT_ROWS=()

slug() {
  printf '%s' "$1" | tr '[:upper:]' '[:lower:]' | sed -E 's/[^a-z0-9]+/-/g; s/^-//; s/-$//'
}

duration() {
  local seconds="$1"
  printf '%02d:%02d:%02d' "$((seconds / 3600))" "$(((seconds % 3600) / 60))" "$((seconds % 60))"
}

record_result() {
  local name="$1"
  local status="$2"
  local elapsed="$3"
  local log_file="$4"
  local exit_code="$5"

  RESULT_ROWS+=("| ${name} | ${status} | $(duration "${elapsed}") | ${exit_code} | ${log_file} |")
}

run_step() {
  local name="$1"
  shift
  local log_file="${REPORT_DIR}/$(slug "${name}").log"
  local started
  local ended
  local elapsed
  local exit_code

  TOTAL_COUNT=$((TOTAL_COUNT + 1))
  echo
  echo "==> ${name}"
  echo "    log: ${log_file}"

  started="$(date +%s)"
  (
    cd "${ROOT_DIR}" || exit 1
    echo "\$ $*"
    "$@"
  ) >"${log_file}" 2>&1
  exit_code=$?
  ended="$(date +%s)"
  elapsed=$((ended - started))

  if [[ "${exit_code}" -eq 0 ]]; then
    PASS_COUNT=$((PASS_COUNT + 1))
    record_result "${name}" "PASS" "${elapsed}" "${log_file}" "${exit_code}"
    echo "    PASS ($(duration "${elapsed}"))"
  else
    FAIL_COUNT=$((FAIL_COUNT + 1))
    record_result "${name}" "FAIL" "${elapsed}" "${log_file}" "${exit_code}"
    echo "    FAIL ($(duration "${elapsed}"), exit ${exit_code})"
  fi
}

skip_step() {
  local name="$1"
  local reason="$2"

  SKIP_COUNT=$((SKIP_COUNT + 1))
  record_result "${name}" "SKIP: ${reason}" 0 "-" "-"
  echo
  echo "==> ${name}"
  echo "    SKIP: ${reason}"
}

docker_available() {
  command -v docker >/dev/null 2>&1 && docker ps >/dev/null 2>&1
}

run_step "focused core queue-map checks" \
  mvn -pl nishi-utils-core \
    -Dtest=NQueueTest,NQueueTruncateTest,DistributedQueueConsumerLocalTest,QueueRestartConsistencyTest,QueueCatchUpIntegrationTest,QueueNodeFailoverIntegrationTest,ConsistencyIntegrationTest,MapNodeFailoverIntegrationTest,NGridMapPersistenceIntegrationTest \
    test

run_step "in-process resilience profile" \
  mvn test -Presilience -Dsurefire.rerunFailingTestsCount=1

if [[ "${INCLUDE_DOCKER}" == true ]]; then
  if docker_available; then
    if [[ "${DOCKER_MODE}" == full ]]; then
      run_step "docker resilience profile" \
        env TESTCONTAINERS_RYUK_DISABLED=true \
        mvn verify -Pdocker-resilience
    else
      run_step "docker map integration tests" \
        env TESTCONTAINERS_RYUK_DISABLED=true \
        mvn verify -pl ngrid-test \
          -Dngrid.test.docker=true \
          "-Dit.test=NGridMap*IT" \
          -DfailIfNoTests=false

      run_step "docker queue durability tests" \
        env TESTCONTAINERS_RYUK_DISABLED=true \
        mvn verify -pl ngrid-test \
          -Dngrid.test.docker=true \
          "-Dit.test=NGridQueue*IT,NGridDurabilityTest,NGridTestcontainersSmokeTest" \
          -DfailIfNoTests=false
    fi
  else
    skip_step "docker gates" "docker is not available"
  fi
else
  skip_step "docker gates" "not requested"
fi

if [[ "${INCLUDE_SOAK}" == true ]]; then
  run_step "soak ${SOAK_MINUTES}m ${SOAK_OPS}ops" \
    mvn test -Psoak \
      -Dngrid.soak.enabled=true \
      -Dngrid.soak.durationMinutes="${SOAK_MINUTES}" \
      -Dngrid.soak.opsPerSecond="${SOAK_OPS}"
else
  skip_step "soak" "not requested"
fi

if [[ "${TOTAL_COUNT}" -gt 0 ]]; then
  PASS_RATE=$((PASS_COUNT * 100 / TOTAL_COUNT))
else
  PASS_RATE=0
fi

{
  echo "# E2E Test Report"
  echo
  echo "- Generated: $(date -Is)"
  echo "- Pass rate: ${PASS_RATE}% (${PASS_COUNT}/${TOTAL_COUNT} executed gates)"
  echo "- Failed: ${FAIL_COUNT}"
  echo "- Skipped: ${SKIP_COUNT}"
  echo "- Strict mode: ${STRICT}"
  echo
  echo "| Gate | Status | Duration | Exit | Log |"
  echo "|---|---:|---:|---:|---|"
  printf '%s\n' "${RESULT_ROWS[@]}"
} >"${REPORT_FILE}"

echo
echo "==> Summary"
echo "    pass rate: ${PASS_RATE}% (${PASS_COUNT}/${TOTAL_COUNT})"
echo "    failed: ${FAIL_COUNT}"
echo "    skipped: ${SKIP_COUNT}"
echo "    report: ${REPORT_FILE}"

if [[ "${STRICT}" == true && "${FAIL_COUNT}" -gt 0 ]]; then
  exit 1
fi

exit 0
