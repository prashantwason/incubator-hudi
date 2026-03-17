#! /bin/bash

setSonarWrapperEnv() {
  PROJECT_ROOT=${PROJECT_ROOT:-$(git rev-parse --show-toplevel)}
  export MONOREPO_KEY="micro"

  # Set sonarconfig.yaml file path to variable
  export SONAR_CONFIG_FILE=${SONAR_CONFIG_FILE:-"$PROJECT_ROOT/sonarqube/config/sonarconfig.yaml"}
  SONAR_OUTPUT_DIR="$(grep sonarw_output_dir "$SONAR_CONFIG_FILE" | awk -F ':' '{print $2}' | xargs)"
  export SONAR_OUTPUT_DIR
  export SONARQUBE_ENVIRONMENT=${SONARQUBE_ENVIRONMENT:-"production"}
  export SONARQUBE_HIERARCHY_TYPE=${SONARQUBE_HIERARCHY_TYPE:-"directory+uown+hybrid"}
  export SONARQUBE_NAMESPACE=${NAMESPACE:-'sonarw'}
  if [[ "$SONARQUBE_ENVIRONMENT" == "staging" && -n "${BUILDKITE_BRANCH}" ]]; then
    export SONARQUBE_NAMESPACE="${SONARQUBE_NAMESPACE}-${BUILDKITE_BRANCH}"
  fi
  if [[ ${BUILDKITE} ]]
  then
  export BUILD_URL="${BUILDKITE_BUILD_URL}#${BUILDKITE_JOB_ID}"
  export GIT_COMMIT="${BUILDKITE_COMMIT}"
  fi
}

setSonarQubeURL() {
  if [[ ! -z "${CERBERUS_PATH}" ]]; then
    if [ "$SONARQUBE_ENVIRONMENT" == "staging" ]; then
      export SONARQUBE_URL="http://localhost:18812"
    elif [ "$SONARQUBE_ENVIRONMENT" == "production" ]; then
      export SONARQUBE_URL="http://localhost:19804"
    fi
  fi
}

startCerberus() {
  CERBERUS_PATH=$(command -v cerberusctl || true)

  if [[ ! -z "${CERBERUS_PATH}" ]]; then
    cerberusctl add -s uown,sonarqube-staging,sonarqube-production
  fi
}
