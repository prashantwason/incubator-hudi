#!/bin/bash
# This file is supposed to run when a diff is landed to master
# This will run full build/testing sequentially to publish code coverage report
set -x
set -o pipefail
set +e

source .buildkite/udj/util.sh

echo "Inside full_build_and_test.sh file"
echo "Print env var $CONFIG"

echo "Test type is $TEST_TYPE"

if [ "$TEST_TYPE" -eq 0 ]
then
  run_functional_test_with_plan_id
  RES=$?
  if [ $RES -ne 0 ]
  then
    echo "Functional tests execution failed."
  fi
elif [ "$TEST_TYPE" -eq 2 ]; then
  run_functional_test_module_with_plan_id
  RES=$?
  if [ $RES -ne 0 ]; then
    echo "Functional tests execution failed."
  fi
else
  # if no value for TEST_TYPE is provided, maintain the current behaviour i.e., run unit tests
  run_unittests_by_plan_id
  RES=$?
  if [ $RES -ne 0 ]
  then
    echo "Unit tests execution failed."
  fi
fi

# Create surefire report, this is useful in finding the failed tests and the time taken by individual tests
create_surefire_report

echo "Calculating module-wise top tests"
log_modules_top_tests
echo "Top tests by duration is computed"

if [ $RES -ne 0 ]
  then
    exit 1
fi

if [ $RES -ne 0 ]
  then
    exit 1
fi
