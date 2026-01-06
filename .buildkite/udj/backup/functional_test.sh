#!/bin/bash
# This file is only to build the entire hoodie oss and run functional tests in every diff
set -x
set -o pipefail
set -e

source .buildkite/udj/util.sh

# Clean and build
build_hoodie_without_checkstyle
echo "Build without checkstyle check successful. Proceed to functional tests"
run_functional_test
echo "Functional tests execution completed."

echo "Calculating module-wise top tests"
log_modules_top_tests
echo "Top tests by duration is computed"