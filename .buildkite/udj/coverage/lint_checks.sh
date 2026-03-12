#!/bin/bash
set -euo pipefail

export JAVA_HOME=$JAVA_11_HOME
export JDK_HOME=$JAVA_HOME
export PATH=$JAVA_HOME/bin:$PATH
export MAVEN_HOME=$MAVEN_3_6_3_HOME
export PATH=$MAVEN_HOME/bin:$PATH

rm -rf /home/udocker/.m2/repository/org/apache/hudi/
mvn checkstyle:check
