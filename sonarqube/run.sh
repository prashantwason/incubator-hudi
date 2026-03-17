#! /bin/bash
set -eo pipefail

set -x

PROJECT_ROOT="${PROJECT_ROOT:-$(git rev-parse --show-toplevel)}"

#Set environment variable required for sonarwrapper and start cerberus with required services
source "$PROJECT_ROOT/sonarqube/sonar_env_setter.sh"
setSonarWrapperEnv
setSonarQubeURL
export SONARQUBE_URL="http://localhost:19804"

export COVERAGE_PATH="build/coverage"

# Script that involves executing unit test and generating coverage report to be added here.
# Coverage report file path should be added in sonarconfig.yaml in sonarqube.unit_test_coverage.location
source "$PROJECT_ROOT/.buildkite/udj/coverage/helper.sh"

download_and_aggregate_reports
jacoco_report

# if env UPLOAD_COVERAGE_TO_PHAB exists run below method to upload coverage to phabricator
if [ -n "$UPLOAD_COVERAGE_TO_PHAB" ]; then
  upload_to_phab
  exit 0
else
  echo "Skipping phabricator upload as UPLOAD_COVERAGE_TO_PHAB is not set"
fi

#Download sonar-wrapper with given version and fetch sonar-scanner maven coords for both os system
source "$PROJECT_ROOT/sonarqube/sonarw_importer.sh"
importSonarW
if [ ! -f "$SONAR_WRAPPER_PEX" ]; then
  echo "Unable to download the PEX file"
  exit 1
fi


# download the appropriate sonar-scanner binary
CLI="linux"
SONAR_SCANNER_ZIP="$(import "$SONAR_SCANNER_LINUX")"
if [ "$(uname)" == "Darwin" ]; then
    SONAR_SCANNER_ZIP="$(import "$SONAR_SCANNER_MACOS")"
    CLI="macosx"
fi

if [ ! -f "$SONAR_SCANNER_ZIP" ]; then
    echo"Unable to download sonar scanner from artifactory"
    exit 1
fi

SONAR_SCANNER_ZIP_LOCATION=$(cd "$(dirname "$SONAR_SCANNER_ZIP")" && pwd)
unzip -od "$SONAR_SCANNER_ZIP_LOCATION" "$SONAR_SCANNER_ZIP"
SONAR_SCANNER="$SONAR_SCANNER_ZIP_LOCATION/sonar-scanner-$SONAR_SCANNER_VERSION-$CLI-x64/bin/sonar-scanner"

if [ ! -f "$SONAR_SCANNER" ]; then
     echo "Unable to find sonar-scanner binary in the archive downloaded from artifactory"
     exit 1
fi
chmod +x "$SONAR_SCANNER"

#Setup python with version 3.9.
echo "--- Set python 3.9.2..."
source "$PROJECT_ROOT/sonarqube/setup_python.sh"

#Start SonarWrapper
#Execute sonar-wrapper commit command with following arguments to upload coverage data
# Do not edit the monorepo-key, namespace config in scripts.
# config: Path to the SonarQube config file
# monorepo-key: The repo key [For micro-repo it will be "micro"]
# project-root: The project root directory
# namespace: The unique prefix for projects and portfolios
# work-unit-file: The absolute path of service that needs to be analyzed.
echo "--- Start SonarWrapper..."
"$SONAR_WRAPPER_PEX" commit \
  --scanner "$SONAR_SCANNER" \
  --project-root "$PROJECT_ROOT" \
  --monorepo-key "$MONOREPO_KEY" \
  --env "$SONARQUBE_ENVIRONMENT" \
  --config "$SONAR_CONFIG_FILE" \
  --work-unit-file "$PROJECT_ROOT"
RETURN_CODE=$?

if [ $RETURN_CODE -ne 0 ]; then
  echo "Error occurred executing the PEX file"
  exit 1
fi

SONARQUBE_OUT_DIR="$(grep sonarw_output_dir "$SONAR_CONFIG_FILE" | awk '{print $2}' | sed "s/'//g")"
SONARQUBE_OUT_DIR="$PROJECT_ROOT/$SONARQUBE_OUT_DIR"

#Upload sonarw.log and sonarw-status to buildkite
buildkite-agent artifact upload "$SONARQUBE_OUT_DIR/sonarw-projects-log.zip" || true
buildkite-agent artifact upload "$SONARQUBE_OUT_DIR/sonarw.log" || true
buildkite-agent artifact upload "$SONARQUBE_OUT_DIR/sonarw-status.csv" || true
