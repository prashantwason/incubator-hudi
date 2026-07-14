#!/usr/bin/env bash
set -euo pipefail

REPO="DAHOOQD"
APP_NAME="hudi_spark_integ_test"
DROGON_CONF="drogon/hudi_spark_integ_test.drogon.json"
TAG="hudi-spark-integration-test-hudi-owned-1.2"
TEST_CONFIG_SRC="drogon/hudi-spark-integ-test/test-config.yaml"
TEST_CONFIG_DEST="/staging/hudi/spark_integ_test/hudi_owned/1.2/test-config.yaml"

echo "==> Installing drogon-cli..."
python3 -m pip install --user drogon-cli
export PATH="$(python3 -m site --user-base)/bin:${PATH}"
export UBER_LDAP_UID="hudi"
export DROGON_CLI_USSO_OVERRIDE="${DROGON_USSO_TOKEN}"

GIT_HASH=$(git rev-parse HEAD)
echo "==> Using git hash: ${GIT_HASH}"

echo "==> Setting drogon app tag..."
drogon set-app-tag \
  -a "${APP_NAME}" \
  --repo "${REPO}" \
  --conf-path "${DROGON_CONF}" \
  --tag "${TAG}" \
  --git-hash "${GIT_HASH}"

echo "==> Uploading test config to TB..."
tb-cli put "${TEST_CONFIG_SRC}" "${TEST_CONFIG_DEST}"

echo "==> Done! test-config.yaml published to ${TEST_CONFIG_DEST}"
