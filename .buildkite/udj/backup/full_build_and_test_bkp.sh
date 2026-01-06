#!/bin/bash
# This file is supposed to run when a diff is landed to master
# This will run full build/testing sequentially to publish code coverage report
set -x
set -o pipefail
set -e

source .buildkite/udj/util.sh

build_hoodie
run_unittest
run_functional_test
#jacoco_report
create_surefire_report

echo "Calculating module-wise top tests"
log_modules_top_tests
echo "Top tests by duration is computed"