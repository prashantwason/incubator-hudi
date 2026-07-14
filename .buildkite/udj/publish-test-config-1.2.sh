#!/usr/bin/env bash
set -euo pipefail

TEST_CONFIG_SRC="drogon/hudi-spark-integ-test/test-config.yaml"
TEST_CONFIG_DEST="/staging/hudi/spark_integ_test/hudi_owned/1.2/test-config.yaml"

echo "==> Uploading test config to TB..."
tb-cli put "${TEST_CONFIG_SRC}" "${TEST_CONFIG_DEST}"

echo "==> Done! test-config.yaml published to ${TEST_CONFIG_DEST}"
