#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
LOG_DIR="${SCRIPT_DIR}/logs"

mkdir -p "${LOG_DIR}"

JUL_CONFIG="${SCRIPT_DIR}/logging.properties"

cd "${ROOT_DIR}"

echo "==> Running NGrid resilience tests"

env MAVEN_OPTS="-Djava.util.logging.config.file=${JUL_CONFIG}" \
  mvn -Dtest=QueueCatchUpIntegrationTest,CatchUpIntegrationTest,QueueNodeFailoverIntegrationTest,LeaderReelectionIntegrationTest \
  test

echo "==> Logs written to ${LOG_DIR}"
