#!/bin/bash

ACTIVE_PROFILES=" -Pspark3.3 -Pflink1.18 -Dspark3.3 -Dflink1.18"
# Set MAVEN_OPTS at a single place to avoid OOM errors and for retries in case of network issues.
# Dont override these properties as they contain logic for retries and timeouts.
export MAVEN_OPTS="-Xms2g -Xmx8g -Dmaven.wagon.http.retryHandler.count=10 -Daether.connector.http.connectionMaxTtl=25 -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false -Djacoco.skip=true"

build_command_to_run=""
test_command_to_run=""

# This method does not fail instead it returns 1 for error.
check_jars_correctness()
{
  set +e
  SPARK_BUNDLE=`find packaging/hudi-spark-bundle/target/ -name  "hudi-spark*.jar" | grep -v sources`
  RES=`jar tf $SPARK_BUNDLE | grep services | grep -v org.apache.spark.sql.sources.DataSourceRegister | grep -v "META-INF/services/$" | grep -v org.apache.hudi | grep -v 'com/uber/hudi' | wc -l`
  if [ $RES -ne 0 ]
  then
    echo "Validation for unwanted services failed."
    echo "$RES Unwanted services found in spark bundle"
    return 1
  fi
  echo "Validation for unwanted services is successful."


  # validate no avsc files are included
  RES=`jar tf $SPARK_BUNDLE | grep \.avsc$ | wc -l`
  if [ $RES -ne 0 ]
  then
    echo "Validation for unwanted avsc files failed."
    echo "$RES Unwanted avsc files found in spark bundle"
    return 1
  fi
  echo "Validation for unwanted avsc files is successful."

  RES=$(jar tf "$SPARK_BUNDLE" | grep '\.class$' | grep -v -E '^(org/apache/hudi/|com/uber/hoodie|org/apache/spark|shaded|META-INF/versions|org/apache/parquet/Hoodie)' | wc -l || true)
  if [ "$RES" -ne 0 ]
  then
    echo "Validation for shading classes in spark bundle failed."
    echo "$RES classes are not shaded"
    jar tf "$SPARK_BUNDLE" | grep '\.class$' | grep -v -E '^(org/apache/hudi/|com/uber/hoodie|org/apache/spark|shaded|META-INF/versions|org/apache/parquet/Hoodie)'
    return 1
  fi
  echo "Validation for shading classes in spark bundle is successful."
  RES=`jar tf $SPARK_BUNDLE | grep hbase-default.xml | wc -l`
  if [ $RES -ne 0 ]
  then
    echo "hbase-default.xml file is included as part of hudi-spark bundle."
    echo "$RES occurrences of hbase-default.xml found in hudi-spark bundle"
    return 1
  fi
  echo "Validation for hbase-default.xml in spark bundle is successful."

  #TODO refactor this code so that it doesn't pollute the main test.sh and have shading tests separate and sourced on demand
  #TODO fix all shell check warnings

   echo "Disabling the shading checks as the shading that was done previously is not needed anymore
   and shading for metrics is also removed. So, removing shading validation for both presto and
   hadoop-mr bundles."

#  PRESTO_BUNDLE="packaging/hudi-presto-bundle/target/hudi-presto-bundle-$CURRENT_VERSION.jar"
#  HIVE_BUNDLE="packaging/hudi-hadoop-mr-bundle/target/hudi-hadoop-mr-bundle-$CURRENT_VERSION.jar"
#
#  JARS_TO_CHECK=("$PRESTO_BUNDLE" "$HIVE_BUNDLE")
#
#  for jar in "${JARS_TO_CHECK[@]}"
#  do
#    shaded_codahale_num=$(tar tf "$jar"| grep "class$"| grep -c "org/apache/hudi/com/codahale/metrics")
#    total_codahale_num=$(tar tf "$jar"| grep "class$"| grep -c "com/codahale/metrics")
#    if [ "$shaded_codahale_num" -ne "$total_codahale_num" ]; then
#      echo "Validation count for shaded metrics classes for both presto and hive bundles failed."
#      return 1
#    fi
#  done
#  echo "Validation count for shaded metrics classes for both presto and hive bundles is successful."

  # Check for vulnerable Log4j 1.x classes in all bundle jars.
  # The safe Log4j 2.x bridge (log4j-1.2-api) also ships org/apache/log4j/ classes
  # but always alongside org/apache/logging/log4j/ (Log4j 2.x core).
  # A jar with org/apache/log4j/ but WITHOUT org/apache/logging/log4j/ has the real Log4j 1.x.
  for jar in $(find packaging -name "*.jar" | grep -v -E '(sources|original|javadoc)'); do
    log4j1_count=`jar tf "$jar" | grep -c "org/apache/log4j/.*\\.class" || true`
    if [ "$log4j1_count" -gt 0 ]; then
      log4j2_count=`jar tf "$jar" | grep -c "org/apache/logging/log4j/.*\\.class" || true`
      if [ "$log4j2_count" -eq 0 ]; then
        echo "Vulnerable Log4j 1.x detected in $jar ($log4j1_count classes, no Log4j 2.x bridge)"
        return 1
      fi
    fi
  done
  echo "Validation for no vulnerable Log4j 1.x jars is successful."

  # Validate that bundle JARs do not expose shaded Hudi modules as transitive dependencies.
  # Each bundle shades certain Hudi modules (with Avro relocation) into a fat JAR. If the
  # published POM still lists any of those shaded modules as dependencies, consumers will
  # pull in unshaded copies alongside the bundle, causing AbstractMethodError at runtime
  # due to relocated vs non-relocated Avro types in method signatures.
  #
  # For each bundle's dependency-reduced-pom.xml, we extract which Hudi modules are in its
  # shade artifactSet, then verify none of them appear as dependencies in the published POM.
  LEAKED=$(find packaging -name "dependency-reduced-pom.xml" -path "*/hudi-*bundle*/target/*" 2>/dev/null \
    | xargs python3 -c "
import xml.etree.ElementTree as ET, sys
NS = 'http://maven.apache.org/POM/4.0.0'
ns = {'m': NS}
failed = False
for pom_path in sys.argv[1:]:
    tree = ET.parse(pom_path)
    root = tree.getroot()
    # Collect shaded hudi modules from the top-level shade artifactSet (not profile-gated)
    shaded = set()
    for plugin in root.findall('.//m:build/m:plugins/m:plugin', ns):
        aid = plugin.find('m:artifactId', ns)
        if aid is not None and aid.text == 'maven-shade-plugin':
            for inc in plugin.iter('{%s}include' % NS):
                text = inc.text or ''
                if text.startswith('org.apache.hudi:'):
                    shaded.add(text.split(':')[1])
    if not shaded:
        continue
    # Check only top-level dependencies (not profile deps which are conditionally activated)
    deps_elem = root.find('m:dependencies', ns)
    if deps_elem is None:
        continue
    for dep in deps_elem.findall('m:dependency', ns):
        aid = dep.find('m:artifactId', ns)
        gid = dep.find('m:groupId', ns)
        if aid is not None and gid is not None and gid.text == 'org.apache.hudi' and aid.text in shaded:
            bundle = pom_path.replace('packaging/', '').split('/target/')[0]
            print(f'{bundle}: leaked shaded module {aid.text}')
            failed = True
sys.exit(1 if failed else 0)
")
  if [ $? -ne 0 ]; then
    echo "FAILED: Shaded modules found as dependencies in bundle published POMs:"
    echo "$LEAKED"
    echo "This will cause unshaded JARs to leak onto the classpath and may cause AbstractMethodError."
    return 1
  fi
  echo "Validation for no leaked shaded modules in all bundles is successful."

  return 0
}

check_validation() {
  # Check if spark bundle is correctly shaded
  check_jars_correctness
  RES=$?
  if [ $RES -ne 0 ]
  then
    echo "Validation for jars correctness failed."
    exit 1
  else
    echo "Validation for jars correctness is successful."
    return 0
  fi
}

build_hoodie() {
  HUDI_QUIETER_LOGGING=1 mvn compile test-compile install ${ACTIVE_PROFILES} -DskipTests -DskipITs -B  | tee build.log
  if [ $? -eq 0 ]; then
    echo "Build succeeded."
    return 0
  else
    echo "Build failed."
    exit 1
  fi
}

build_hoodie_without_checkstyle() {
  HUDI_QUIETER_LOGGING=1 mvn compile test-compile install ${ACTIVE_PROFILES} -DskipTests -DskipITs -Dcheckstyle.skip -Dscalastyle.skip -Drat.skip=true -B | tee build.log
  if [ $? -eq 0 ]; then
    echo "Build without checkstyle check succeeded."
    return 0
  else
    echo "Build without checkstyle check failed."
    exit 1
  fi
}

run_unittest() {
  set +e
  # Run unit tests in parallel
  mvn test -Punit-tests ${ACTIVE_PROFILES} -DtrimStackTrace=false -DreuseForks=false -Dcheckstyle.skip=true -pl !hudi-client/hudi-spark-client,!hudi-common,!hudi-utilities, -B >unittest_log1.txt 2>&1 &
  PIDS[0]=$!
  mvn test -Punit-tests ${ACTIVE_PROFILES} -DtrimStackTrace=false -DreuseForks=false -Dcheckstyle.skip=true -pl hudi-utilities,hudi-common -B >unittest_log2.txt 2>&1 &
  PIDS[1]=$!
  mvn test -Punit-tests ${ACTIVE_PROFILES} -DtrimStackTrace=false -DreuseForks=false -Dcheckstyle.skip=true -pl hudi-client/hudi-spark-client -B >unittest_log3.txt 2>&1 &
  PIDS[2]=$!

  # Wait for completion
  ERR=0
  for pid in ${PIDS[*]}; do
    wait $pid
    if [ $? -ne 0 ]; then
      echo "FAILED"
      ERR=1
    fi
  done

  if [ $ERR -eq 1 ]
  then
    echo "Unit tests execution failed."
    return $ERR
  fi

  echo "Successfully completed unit tests in all modules without failures."
  return 0
}

run_unittests_by_plan_id() {
  set +e
  echo "Plan id provided is $PLAN_ID"
  echo "Print env var $CONFIG"

  class_specific_command=""
  # package_specific_command=""
  set_commands "unit-tests"

  start=$SECONDS
  eval "$build_command_to_run"

  # Wait until the background process is completed.
  PID=$!
  wait $PID
  if [ $? -eq 0 ]; then
    echo "Build succeeded"
  else
    echo "Build failed"
    return 1
  fi

  duration=$(( SECONDS - start ))
  echo "Build took $duration seconds to run."
  echo "Starting test command $test_command_to_run..."

  start=$SECONDS
  eval "$test_command_to_run"

  # Wait until the background process is completed.
  PID=$!
  wait $PID
  if [ $? -eq 0 ]; then
    echo "Successfully ran all tests"
    return_status=0
  else
    echo "Some tests have failed"
    return_status=1
  fi
  duration=$(( SECONDS - start ))
  echo "Test took $duration seconds to execute."

  if [ "$UPLOAD_TO_TB" == "true" ]; then
    source .buildkite/udj/coverage/helper.sh
    echo "UPLOAD_TO_TB is set to true. proceeding to upload to terrablob."
    upload_ut_coverage_report_to_tb
  fi

  upload_surefire_reports
  return $return_status
}

run_functional_test() {
  test_command_to_run="mvn test -Pfunctional-tests ${ACTIVE_PROFILES} -DreuseForks=false -DtrimStackTrace=false -Drat.skip=true -Dcheckstyle.skip=true -Dscalastyle.skip -fae -B > functional_test_log.txt 2>&1 &"
  start=$SECONDS
  eval "$test_command_to_run"

  # Wait until the background process is completed.
  PID=$!
  wait $PID
  if [ $? -eq 0 ]; then
    echo "Successfully ran all functional tests"
    return_status=0
  else
    echo "Some functional tests have failed"
    return_status=1
  fi
  duration=$(( SECONDS - start ))
  echo "Test took $duration seconds to execute."
  return $return_status
}

run_functional_test_with_plan_id() {
  set +e
  echo "Starting execution of functional tests."
  if [ "$PLAN_ID" -eq 0 ]
  then
    # Run functional tests for rest of the modules.
    build_command_to_run="mvn compile test-compile install -Pwarn-log $ACTIVE_PROFILES -DskipTests -DskipITs -Drat.skip=true -Dcheckstyle.skip=true -Dscalastyle.skip -B | tee build.log"
    test_command_to_run="mvn test -Pfunctional-tests ${ACTIVE_PROFILES} -DreuseForks=false -DtrimStackTrace=false -Drat.skip=true -Dcheckstyle.skip=true -Dscalastyle.skip -pl !hudi-client/hudi-spark-client,!hudi-cli -fae -B > functional_test_log.txt 2>&1 &"
  elif [ "$PLAN_ID" -eq 1 ]
  then
    # Run functional tests on hudi-client/hudi-spark-client
    modules_to_execute="hudi-client/hudi-spark-client"
  elif [ "$PLAN_ID" -eq 2 ]
  then
    # Run functional tests for hudi-cli module
    modules_to_execute="hudi-cli"
  fi

  if [ "$PLAN_ID" -gt 0 ]
  then
    build_command_to_run="mvn compile test-compile install -Pwarn-log $ACTIVE_PROFILES -DskipTests -DskipITs -Drat.skip=true -Dcheckstyle.skip=true -Dscalastyle.skip -pl $modules_to_execute -am -B | tee build.log"
    test_command_to_run="mvn test -Pfunctional-tests ${ACTIVE_PROFILES} -DreuseForks=false -DtrimStackTrace=false -Drat.skip=true -Dcheckstyle.skip=true -Dscalastyle.skip -pl $modules_to_execute -fae -B > functional_test_log.txt 2>&1 &"
  fi

  start=$SECONDS
  eval "$build_command_to_run"

  # Wait until the background process is completed.
  PID=$!
  wait $PID
  if [ $? -eq 0 ]; then
    echo "Build succeeded"
  else
    echo "Build failed"
    return 1
  fi
  duration=$(( SECONDS - start ))
  echo "Build took $duration seconds to run."
  echo "Starting test command $test_command_to_run..."

  start=$SECONDS
  eval "$test_command_to_run"

  # Wait until the background process is completed.
  PID=$!
  wait $PID
  if [ $? -eq 0 ]; then
    echo "Successfully ran all functional tests"
    return_status=0
  else
    echo "Some functional tests have failed"
    return_status=1
  fi
  duration=$(( SECONDS - start ))
  echo "Test took $duration seconds to execute."

  if [ "$UPLOAD_TO_TB" == "true" ]; then
    source .buildkite/udj/coverage/helper.sh
    echo "UPLOAD_TO_TB is set to true. proceeding to upload to terrablob."
    upload_ft_coverage_report_to_tb
  fi
  upload_surefire_reports
  return $return_status
}

# Similar to run_functional_test_with_plan_id but here we run tests for each of module separately
run_functional_test_module_with_plan_id() {
  set +e
  echo "Starting execution of functional tests with plan_id."

  # based on plan_id, set module to execute.
  set_commands "functional-tests"
  start=$SECONDS
  eval "$build_command_to_run"

  # Wait until the background process is completed.
  PID=$!
  wait $PID
  if [ $? -eq 0 ]; then
    echo "Build succeeded"
  else
    echo "Build failed"
    return 1
  fi
  duration=$(( SECONDS - start ))
  echo "Build took $duration seconds to run."
  echo "Starting test command $test_command_to_run..."

  start=$SECONDS
  eval "$test_command_to_run"

  # Wait until the background process is completed.
  PID=$!
  wait $PID
  if [ $? -eq 0 ]; then
    echo "Successfully ran all functional tests"
    return_status=0
  else
    echo "Some functional tests have failed"
    return_status=1
  fi
  duration=$(( SECONDS - start ))
  echo "Test took $duration seconds to execute."
  source .buildkite/udj/coverage/helper.sh
  echo "Proceeding to upload functional coverage result to terrablob."
  upload_ft_coverage_report_to_tb
  upload_surefire_reports
  return $return_status
}

log_modules_top_tests() {
  echo "Logging the top test times"
  python3 .buildkite/udj/modules_top_tests.py True True 5 20 >modules_top_tests.txt 2>&1
  echo "Finished logging test times"
}

create_surefire_report() {
  output_file="all-surefire-reports.txt"
  echo $output_file
  find . -type d -name "surefire-reports" | while read -r dir; do
      find "$dir" -type f -name "*.txt" | while read -r file; do
          cat "$file" >> "$output_file"
      done
  done
}

# Based on PLAN_ID, set the modules to execute and the commands to run.
set_commands() {
  local test_profile=$1
  # Initialize flags for test execution mode
  use_surefire_goal_only=""
  skip_surefire=""
  case $PLAN_ID in
    0)
      build_command_to_run="mvn compile test-compile install -Pwarn-log $ACTIVE_PROFILES -DskipTests -DskipITs -Drat.skip=true -Dcheckstyle.skip=true -Dscalastyle.skip -B | tee build.log"
      test_command_to_run="mvn test -P$test_profile $ACTIVE_PROFILES -DreuseForks=false -DtrimStackTrace=false -Drat.skip=true -Dcheckstyle.skip=true -Dscalastyle.skip -pl !hudi-client/hudi-spark-client,!hudi-common,!hudi-utilities, -fae -B > unittest_logs.txt 2>&1 &"
      ;;
    1)
      modules_to_execute="hudi-common"
      ;;
    2)
      modules_to_execute="hudi-spark-datasource,hudi-spark-datasource/hudi-spark"
      ;;
    3)
      modules_to_execute="hudi-utilities"
      class_to_skip1="org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamerSchemaEvolutionQuick"
      class_to_skip2="org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer"
      class_specific_command="-Dtest=!${class_to_skip1},!${class_to_skip2}"
      ;;
    4)
      modules_to_execute="hudi-flink-datasource,hudi-flink-datasource/hudi-flink,hudi-flink-datasource/hudi-flink1.17.x"
      ;;
    5)
      modules_to_execute="hudi-client/hudi-spark-client"
      ;;
    6)
      modules_to_execute="hudi-spark-datasource/hudi-spark3-common"
      ;;
    7)
      modules_to_execute="hudi-hadoop-mr"
      ;;
    8)
      modules_to_execute="hudi-uber"
      ;;
    9)
      modules_to_execute="hudi-sync/hudi-hive-sync"
      ;;
    10)
      modules_to_execute="hudi-client/hudi-java-client"
      ;;
    11)
      modules_to_execute="hudi-client/hudi-client-common"
      ;;
    12)
      modules_to_execute="hudi-client/hudi-flink-client"
      ;;
    13)
      modules_to_execute="hudi-utilities"
      class_to_include="org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamerSchemaEvolutionQuick"
      class_specific_command="-Dtest=${class_to_include}"
      ;;
    14)
      modules_to_execute="hudi-utilities"
      class_to_include="org.apache.hudi.utilities.deltastreamer.TestHoodieDeltaStreamer"
      class_specific_command="-Dtest=${class_to_include}"
      ;;
    17)
      modules_to_execute="hudi-spark-datasource/hudi-spark3.3.x"
      ;;
    18)
      modules_to_execute="hudi-spark-datasource/hudi-spark3.2plus-common"
      ;;
    19)
      modules_to_execute="hudi-spark-datasource/hudi-spark-common"
      ;;
    20)
      # PLAN_ID 20: Run only Surefire (Java/JUnit) tests for hudi-spark module
      # ScalaTest is skipped by using surefire:test goal directly instead of test phase
      # This avoids CI timeout by splitting hudi-spark tests into two separate jobs
      modules_to_execute="hudi-spark-datasource/hudi-spark"
      use_surefire_goal_only="true"
      ;;
    21)
      modules_to_execute="hudi-utilities"
      ;;
    22)
      modules_to_execute="hudi-cli"
      ;;
    23)
      modules_to_execute="hudi-kafka-connect"
      ;;
    24)
      modules_to_execute="hudi-sync/hudi-sync-common"
      ;;
    25)
      modules_to_execute="hudi-timeline-service"
      ;;
    26)
      modules_to_execute="hudi-flink-datasource/hudi-flink"
      ;;
    27)
      modules_to_execute="hudi-flink-datasource/hudi-flink1.18.x"
      ;;
    28)
      modules_to_execute="hudi-client/hudi-flink-client"
      ;;
    29)
      # PLAN_ID 29: Run only ScalaTest (Scala) tests for hudi-spark module
      # Surefire is skipped using -Dsurefire.skip=true
      # This is the companion job to PLAN_ID 20 which runs Surefire-only
      modules_to_execute="hudi-spark-datasource/hudi-spark"
      skip_surefire="true"
      ;;
  esac

  if [ "$PLAN_ID" -gt 0 ]; then
    if [ "$test_profile" == "unit-tests" ]; then
      log_file="unittest_logs.txt"
    else
      log_file="functional_test_log.txt"
    fi
    build_command_to_run="mvn compile test-compile install -Pwarn-log $ACTIVE_PROFILES -DskipTests -DskipITs -Drat.skip=true -Dcheckstyle.skip=true -Dscalastyle.skip -pl $modules_to_execute -am -B > build.log 2>&1 &"

    # Construct test command based on flags
    if [ "$use_surefire_goal_only" == "true" ]; then
      # Run only Surefire tests by invoking surefire:test goal directly (skips ScalaTest)
      test_command_to_run="mvn surefire:test -P$test_profile $ACTIVE_PROFILES -DreuseForks=false -DtrimStackTrace=false -Drat.skip=true -Dcheckstyle.skip=true -Dscalastyle.skip -pl $modules_to_execute $class_specific_command -fae -B > $log_file 2>&1 &"
    elif [ "$skip_surefire" == "true" ]; then
      # Run only ScalaTest by skipping Surefire plugin
      test_command_to_run="mvn test -P$test_profile $ACTIVE_PROFILES -Dsurefire.skip=true -DreuseForks=false -DtrimStackTrace=false -Drat.skip=true -Dcheckstyle.skip=true -Dscalastyle.skip -pl $modules_to_execute $class_specific_command -fae -B > $log_file 2>&1 &"
    else
      # Default: run both Surefire and ScalaTest
      test_command_to_run="mvn test -P$test_profile $ACTIVE_PROFILES -DreuseForks=false -DtrimStackTrace=false -Drat.skip=true -Dcheckstyle.skip=true -Dscalastyle.skip -pl $modules_to_execute $class_specific_command -fae -B > $log_file 2>&1 &"
    fi
  fi
}

# Use JDK 11 and MVN 3.6.3
export JAVA_HOME=$JAVA_11_HOME
export JDK_HOME=$JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH
export MAVEN_HOME=$MAVEN_3_6_3_HOME
export PATH=$MAVEN_HOME/bin:$PATH

javac --version
java --version
mvn --version
