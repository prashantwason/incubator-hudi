#!/bin/bash
# This file is only to build the entire hoodie oss and run unit tests in every diff
set -x
set -o pipefail
set -e

source .buildkite/udj/util.sh

# build_hoodie
echo "Build successful. Proceed to unit tests"
run_unittests_by_plan_id
echo "Unit tests execution completed."

echo "Calculating module-wise top tests"
log_modules_top_tests
echo "Top tests by duration is computed"
