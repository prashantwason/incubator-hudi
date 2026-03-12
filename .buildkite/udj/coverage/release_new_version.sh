#!/bin/bash
set -euo pipefail

source .buildkite/udj/util.sh

ls -la /home/udocker/.m2/
cp ".buildkite/settings.xml" "/home/udocker/.m2/settings.xml"
echo "Printing m2 contents, presence of existing files and settings.xml indicate that the cache is working"
ls -la /home/udocker/.m2/
GIT_SHA1="${GIT_SHA1:-$(git rev-parse HEAD)}"
echo "$GIT_SHA1"
export MAVEN_OPTS="-Xmx2048M -Dmaven.wagon.http.retryHandler.count=10 -Daether.connector.http.connectionMaxTtl=25 -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false"
rm -rf /home/udocker/.m2/repository/org/apache/hudi/
mvn deploy $ACTIVE_PROFILES -DskipITs -DskipTests -DGIT_SHA1="$GIT_SHA1" -DREPO_URL="gitolite@code.uber.internal:data/hoodie_oss.git"
