#!/bin/bash

ACTIVE_PROFILES="-Dscala-2.12 -Pspark3.3 -Pflink1.18 -Pwarn-log -Dspark3.3 -Dflink1.18"

# Modules to build and test, keep this in sync with test_plans.
LIST_OF_MODULES_TO_COVER=(1 5 6 7 8 9 10 11 17 19 20 21 22 24 25 26 27 28)

declare -A test_plans

# Define mapping of expected jacoco.exec location, append at the end for any new modules.
test_plans[1]="hudi-common/target/jacoco"                        # PLAN_ID=1
test_plans[5]="hudi-client/hudi-spark-client/target/jacoco"      # PLAN_ID=5
test_plans[6]="hudi-spark-datasource/hudi-spark3-common/target/jacoco"  # PLAN_ID=6
test_plans[7]="hudi-hadoop-mr/target/jacoco"                     # PLAN_ID=7
test_plans[8]="hudi-uber/target/jacoco"                          # PLAN_ID=8
test_plans[9]="hudi-sync/hudi-hive-sync/target/jacoco"           # PLAN_ID=9
test_plans[10]="hudi-client/hudi-java-client/target/jacoco"      # PLAN_ID=10
test_plans[11]="hudi-client/hudi-client-common/target/jacoco"    # PLAN_ID=11
test_plans[17]="hudi-spark-datasource/hudi-spark3.3.x/target/jacoco"    # PLAN_ID=17
#test_plans[18]="hudi-spark-datasource/hudi-spark3.2plus-common/target/jacoco"    # PLAN_ID=18, enable if any UT added in future.
test_plans[19]="hudi-spark-datasource/hudi-spark-common/target/jacoco"  # PLAN_ID=19
test_plans[20]="hudi-spark-datasource/hudi-spark/target/jacoco"         # PLAN_ID=20
test_plans[21]="hudi-utilities/target/jacoco"         # PLAN_ID=21
test_plans[22]="hudi-cli/target/jacoco"               # PLAN_ID=22
test_plans[24]="hudi-sync/hudi-sync-common/target/jacoco"               # PLAN_ID=24
test_plans[25]="hudi-timeline-service/target/jacoco"               # PLAN_ID=25
test_plans[26]="hudi-flink-datasource/hudi-flink/target/jacoco"               # PLAN_ID=26
test_plans[27]="hudi-flink-datasource/hudi-flink1.18.x/target/jacoco"               # PLAN_ID=27
test_plans[28]="hudi-client/hudi-flink-client/target/jacoco"               # PLAN_ID=28

declare -A module_to_plan_id

# Reverse mapping from module names to PLAN_ID, for readability in CI.
module_to_plan_id["hudi-common"]=1
module_to_plan_id["hudi-client/hudi-spark-client"]=5
module_to_plan_id["hudi-spark-datasource/hudi-spark3-common"]=6
module_to_plan_id["hudi-hadoop-mr"]=7
module_to_plan_id["hudi-uber"]=8
module_to_plan_id["hudi-sync/hudi-hive-sync"]=9
module_to_plan_id["hudi-client/hudi-java-client"]=10
module_to_plan_id["hudi-client/hudi-client-common"]=11
module_to_plan_id["hudi-spark-datasource/hudi-spark3.3.x"]=17
module_to_plan_id["hudi-spark-datasource/hudi-spark-common"]=19
module_to_plan_id["hudi-spark-datasource/hudi-spark"]=20
module_to_plan_id["hudi-utilities"]=21
module_to_plan_id["hudi-cli"]=22
module_to_plan_id["hudi-sync/hudi-sync-common"]=24
module_to_plan_id["hudi-timeline-service"]=25
module_to_plan_id["hudi-flink-datasource/hudi-flink"]=26
module_to_plan_id["hudi-flink-datasource/hudi-flink1.18.x"]=27
module_to_plan_id["hudi-client/hudi-flink-client"]=28

# Define which modules to skip for which test type
declare -A SKIP_COVERAGE_MODULES
# Format: SKIP_COVERAGE_MODULES["ut-<module>"]=1, SKIP_COVERAGE_MODULES["ft-<module>"]=1
for m in 26 27 28; do
  SKIP_COVERAGE_MODULES["ut-$m"]=1
  SKIP_COVERAGE_MODULES["ft-$m"]=1
done
SKIP_COVERAGE_MODULES["ft-5"]=1

# Helper function to check if a module should be skipped for a test type
should_skip() {
  local type="$1"
  local module="$2"
  [[ "${SKIP_COVERAGE_MODULES["$type-$module"]}" == "1" ]]
}

# Download jacoco exec files from Terrablob, place them in their respective module location to mimic local
# unit test execution and then perform aggregate.
download_and_aggregate_reports() {
  unique_dir=$(get_uniquedir)

  echo "Downloading coverage reports from Terrablob"

  for module in "${LIST_OF_MODULES_TO_COVER[@]}"; do
    for type in ut ft; do
      if should_skip "$type" "$module"; then
        echo "Skipping ${type^^} coverage download for module $module"
        continue
      fi
      module_dir="${unique_dir}/${module}"
      mkdir -p "$(dirname "${test_plans[$module]}")"

      if [ "$skip_coverage" = true ]; then
        echo "Skipping download of ${type}.exec for plan $module and creating empty file."
        touch "${test_plans[$module]}-${type}.exec"
      else
        tb-cli --withCerberus get "${module_dir}/jacoco-${type}.exec" "${test_plans[$module]}-${type}.exec"
      fi

      if [ $? -eq 0 ]; then
        echo "Successfully downloaded ${type^^} coverage report for module $module from $module_dir to ${test_plans[$module]}-${type}.exec"
        ls -ltr "${test_plans[$module]}-${type}.exec"
      else
        echo "Failed to download ${type^^} coverage report for module $module from $module_dir"
        exit 1
      fi
    done
  done

  echo "All modules have been downloaded successfully from Terrablob"
}

# Upload jacoco exec to terrablob for each module based on git SHA.
upload_ut_coverage_report_to_tb() {
  unique_dir=$(get_uniquedir)
  echo "Uploading test results to Terrablob $unique_dir"

  path_coverage_report="${modules_to_execute}/target/jacoco-ut.exec"

  # skip coverage for 26 27 28
  if should_skip "ut" "$PLAN_ID"; then
    echo "Skipping coverage upload for module $PLAN_ID"
    return 0
  fi

  if [ -f "$path_coverage_report" ]; then
    echo "Found coverage report at $path_coverage_report"
  else
    echo "Coverage report not found at $path_coverage_report, creating an empty file."
    mkdir -p "$(dirname "$path_coverage_report")"
    touch "$path_coverage_report"
  fi

  tb-cli --withCerberus mkdir -p "${unique_dir}/${PLAN_ID}/"
  module_file="${unique_dir}/${PLAN_ID}/jacoco-ut.exec"

  tb-cli --withCerberus put "$path_coverage_report" "$module_file"
  if [ $? -eq 0 ]; then
    echo "Successfully uploaded coverage report to $module_file"
  else
    echo "Failed to upload coverage report to $module_file"
    exit 1
  fi
}

upload_ft_coverage_report_to_tb() {
  unique_dir=$(get_uniquedir)
  echo "Uploading test results to Terrablob $unique_dir"

  path_coverage_report="${modules_to_execute}/target/jacoco-ft.exec"
  ls -ltr "$path_coverage_report"
  if [ -f "$path_coverage_report" ]; then
    echo "Found coverage report at $path_coverage_report"
  else
    echo "Coverage report not found for functional tests at $path_coverage_report, creating an empty file."
    touch "$path_coverage_report"
  fi

  tb-cli --withCerberus mkdir -p "${unique_dir}/${PLAN_ID}/"
  module_file="${unique_dir}/${PLAN_ID}/jacoco-ft.exec"
  tb-cli --withCerberus put "$path_coverage_report" "$module_file"
  if [ $? -eq 0 ]; then
    echo "Successfully uploaded coverage report to $module_file"
  else
    echo "Failed to upload coverage report to $module_file"
    exit 1
  fi
}

# Aggregate jacoco reports on the main job.
jacoco_report() {
  mvn jacoco:report-aggregate
  echo "Completed generation of jacoco report"

  echo "===================== Aggregate report exists? =========================="
  path_coverage_report="$(pwd)/packaging/hudi-codecoverage/target/site/jacoco-aggregate/jacoco.xml"
  ls -al "$path_coverage_report"

  unique_dir=$(get_uniquedir)
  echo "Uploading aggregated coverage to Terrablob $unique_dir"

  module_file="${unique_dir}/jacoco.xml"
  tb-cli --withCerberus put "$path_coverage_report" "$module_file"
}

get_uniquedir() {
  if [[ ! -z "${PHAB_DIFF_ID-}" ]]; then
    subdir=$PHAB_DIFF_ID
  else
    subdir=$(git rev-parse --short=8 HEAD)
  fi
  echo "/staging/data/hudi/coverage-reports/${subdir}"
}

build_hoodie() {
  HUDI_QUIETER_LOGGING=1 mvn compile test-compile install ${ACTIVE_PROFILES} -DskipTests -DskipITs -Dcheckstyle.skip -Dscalastyle.skip -Drat.skip=true -B | tee build.log
  if [ $? -eq 0 ]; then
    echo "Build succeeded."
    return 0
  else
    echo "Build failed."
    exit 1
  fi
}

has_coverage() {
  local sha=$1
  echo "Checking coverage for SHA: $sha"
  path="/staging/data/hudi/coverage-reports/${sha}/jacoco.xml"
  coverage_base_file="packaging/hudi-codecoverage/target/site/jacoco-aggregate/coverage_base.xml"
  tb-cli --withCerberus get "$path" "$coverage_base_file"
#  tb-cli get "$path" "$coverage_base_file"

  # Check if coverage file exists
  if [ -s "$coverage_base_file" ]; then
    echo "Coverage file exists: $coverage_base_file"
    return 0
  fi
  return 1
}

# Find the base branch SHA where the current branch was created from release
get_base_branch_sha() {
  local current_branch_sha=$1
  local release_branch=$2
  local base_sha
  base_sha=$(git merge-base "$current_branch_sha" "$release_branch" 2>/dev/null)
  base_sha=$(git rev-parse --short=8 "$base_sha")
  echo "$base_sha"
}

# Find a commit with coverage among the last 7 commits of the base branch
find_coverage_sha() {
  local base_branch_sha=$1

  # Get the last 30 days of commits from the base branch SHA, including commit messages
  local commits
  commits=$(git log "$base_branch_sha" --pretty=format:"%h %ci %s" --since="30 days ago" 2>/dev/null)

  echo -e "\nCommits in the order from most recent to oldest :"
  echo "$commits"

  if [ -z "$commits" ]; then
    echo "No commits found in the last 30 days from base branch SHA $base_branch_sha."
    return 1
  fi

  mkdir -p "packaging/hudi-codecoverage/target/site/jacoco-aggregate/"
  while read -r commit_line; do
    [ -z "$commit_line" ] && continue
    local sha
    echo "Checking commit to see if coverage is available: $commit_line"
    sha=$(echo "$commit_line" | awk '{print $1}')  # Extract SHA
    sha=$(git rev-parse --short=8 "$sha" 2>/dev/null)
    [ -z "$sha" ] && continue

    valid_coverage_sha=$(has_coverage "$sha")
    if [[ $? -eq 0 ]]; then
      echo "Valid coverage SHA: $valid_coverage_sha"
      mkdir -p "build/comment/phabricator-comment-code-coverage"
      echo "Baseline commit used for comparison for new line coverage > $commit_line  " > "build/comment/phabricator-comment-code-coverage/newline_coverage.md"
      return 0
    fi
  done <<< "$commits"

  echo "No coverage found in the recent commits."
  return 1
}

# Main logic to find a valid SHA with coverage
get_valid_coverage_sha() {
  local release_branch="origin/release"

  # Get the current branch SHA
  local current_branch_sha
  current_branch_sha=$(git rev-parse --short=8 HEAD)
#  current_branch_sha="cd3eec85" # Hardcoded for testing, HUDI-6082 Improve UT coverage for manager tools: EstimateResources
  #  we are expected to find  670b7068 as the base branch
  echo "Current branch SHA: $current_branch_sha"

  # Get the base branch SHA
  local base_branch_sha
  base_branch_sha=$(get_base_branch_sha "$current_branch_sha" "$release_branch")

  # Check for coverage in the base branch's last 7 commits
  if find_coverage_sha "$base_branch_sha"; then
    return 0
  fi

  echo "No coverage SHA found."
  return 1
}

upload_to_phab() {
  mkdir -p build/comment/phabricator-comment-code-coverage
  mkdir -p packaging/hudi-codecoverage/target/site/jacoco-aggregate/

  get_valid_coverage_sha
  if [ $? -ne 0 ]; then
    echo "No baseline coverage found. Skipping coverage comparison."
    echo "No baseline coverage available for comparison." > "build/comment/phabricator-comment-code-coverage/newline_coverage.md"
    return 0
  fi

  newline_coverage_file="newline_coverage.txt"
  python3 .buildkite/udj/coverage/compare_coverage_file.py "packaging/hudi-codecoverage/target/site/jacoco-aggregate/coverage_base.xml" "packaging/hudi-codecoverage/target/site/jacoco-aggregate/jacoco.xml"
  echo "===================== Phabricator comment files: ========================="
  ls -al `pwd`/build/comment/phabricator-comment-code-coverage/
  return $?
}

# Upload Surefire test reports to Buildkite artifacts
upload_surefire_reports() {
  local surefire_dir="${modules_to_execute}/target/surefire-reports"

  echo "Checking for Surefire test reports in $surefire_dir"
  if [ -d "$surefire_dir" ]; then
    echo "Found Surefire reports directory, uploading reports to Buildkite..."

    # Upload all dump files from the surefire-reports directory
    buildkite-agent artifact upload "$surefire_dir/*.dumpstream"
    buildkite-agent artifact upload "$surefire_dir/*.dump"
    buildkite-agent artifact upload "$surefire_dir/*.txt"

    if [ $? -eq 0 ]; then
      echo "Successfully uploaded Surefire test reports from $surefire_dir"
    else
      echo "Failed to upload Surefire test reports from $surefire_dir"
    fi
  else
    echo "No Surefire test reports found at $surefire_dir"
  fi
}

 # install JDK 11 (required for Spark 3)
 #echo "Installing JDK 11 from https://cdn.azul.com/zulu/bin/zulu11.66.15-ca-jdk11.0.20-linux_x64.tar.gz"
 #wget https://cdn.azul.com/zulu/bin/zulu11.66.15-ca-jdk11.0.20-linux_x64.tar.gz
 #tar xzf zulu11.66.15-ca-jdk11.0.20-linux_x64.tar.gz
 #export JRE_11_PATH=`pwd`/zulu11.66.15-ca-jdk11.0.20-linux_x64/bin/java
 #export JRE_11_DIR=`pwd`/zulu11.66.15-ca-jdk11.0.20-linux_x64/bin/
 #echo "Set JRE_11 executable path to $JRE_11_PATH"
 #echo "Set JRE_11 directory to $JRE_11_DIR"
 #$JRE_11_PATH -version
