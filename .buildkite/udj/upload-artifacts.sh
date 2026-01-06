#!/bin/bash
set -o pipefail
set -e
set -x

git clean -dfx
echo `pwd`

# print mvn configuration
mvn -version
javac -version
java -version

echo $REPO_URL
echo $GIT_SHA1

mvn -DGIT_SHA1="$GIT_SHA1" -DREPO_URL="$REPO_URL" clean deploy --update-snapshots -DskipITs -DskipTests | tee build.log
if [ $? -eq 0 ]; then
  echo "Build and upload to artifactory succeeded"
else
  echo "Build and upload to artifactory failed"
  exit 1
fi

