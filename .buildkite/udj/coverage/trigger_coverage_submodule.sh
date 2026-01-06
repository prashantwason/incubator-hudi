#!/bin/bash
# This file is supposed to run when a diff is landed to master
# This will run full build/testing sequentially to publish code coverage report
set -x
set -o pipefail
set +e

PROJECT_ROOT="${PROJECT_ROOT:-$(git rev-parse --show-toplevel)}"
chmod -R u+rwX "$PROJECT_ROOT"

source $PROJECT_ROOT/.buildkite/udj/util.sh
source .buildkite/udj/util.sh
echo "Printing m2 contents, presence of existing files and settings.xml indicate that the cache is working"
ls -la ~/.m2

source $PROJECT_ROOT/.buildkite/udj/coverage/helper.sh

if [ -z "$BUILDKITE_BRANCH" ]; then
  echo "Error: BUILDKITE_BRANCH environment variable is not set"
  exit 1
fi
git checkout $BUILDKITE_BRANCH
export UPLOAD_TO_TB="true"

# Set PLAN_ID based on MODULE environment variable
if [ -n "$MODULE" ]; then
  echo "Assigning test_id for $MODULE"
  export PLAN_ID="${module_to_plan_id["$MODULE"]}"
  if [ -z "$PLAN_ID" ]; then
    echo "Error: Unknown module '$MODULE'"
    exit 1
  fi
else
  echo "Error: MODULE environment variable is not set"
  exit 1
fi

echo "Set PLAN_ID to $PLAN_ID for module $MODULE"

# create unique directory based on epoch
M2_WRITE_DIR="$PROJECT_ROOT/m2_write_dir/$(date +%s)"
export MAVEN_OPTS="-Xms5g -Xmx10g -Dmaven.repo.local=$M2_WRITE_DIR -Dmaven.wagon.http.retryHandler.count=10 -Daether.connector.http.connectionMaxTtl=25 -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false"

if [ "$TEST_NAME" == "functional-tests" ]; then
  echo "Running functional tests for module $MODULE"
  run_functional_test_module_with_plan_id
  RES=$?
  if [ $RES -ne 0 ] && [ -z "$REPORT_TO_SONARQUBE" ]; then
    echo "Functional tests execution failed."
    exit 1
  fi
elif [ "$TEST_NAME" == "unit-tests" ]; then
  echo "Running unit tests for module $MODULE"
  run_unittests_by_plan_id
  RES=$?
  if [ $RES -ne 0 ] && [ -z "$REPORT_TO_SONARQUBE" ]; then
    echo "Unit tests execution failed."
    exit 1
  fi
else
  echo "Error: Unknown test name '$TEST_NAME'"
  exit 1
fi

ls -al $M2_WRITE_DIR