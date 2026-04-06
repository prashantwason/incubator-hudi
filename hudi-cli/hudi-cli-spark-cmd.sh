#!/usr/bin/env bash

#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.

if [ "$#" -lt 3 ]; then
  echo "Usage: `basename "$0"` <Command> <spark-master> <spark-executor-memory> [... command specific arguments ...]"
  exit -1
fi

# create spark-events directory required for local spark
if [[ $2 = local* ]]
then
    mkdir -p /tmp/spark-events
fi


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
HOODIE_JAR=`ls $DIR/target/hudi-cli-*.jar | grep -v source | grep -v javadoc`

. "${DIR}"/conf/hudi-env.sh

if [ -z "$CLIENT_JAR" ]; then
  echo "Client jar location not set, please set it in conf/hudi-env.sh"
  exit 1
fi

# To access CFS/GS paths, we need the internal Uber hadoop distribution.
# Place it first on the classpath so its CFS/GCS classes take precedence.
INTERNAL_HADOOP_DIR=${DIR}/target/internal-hadoop
mkdir -p ${INTERNAL_HADOOP_DIR}
INTERNAL_HADOOP_JAR=${INTERNAL_HADOOP_DIR}/hadoop-common-2.8.2.53.jar
if [ ! -f "${INTERNAL_HADOOP_JAR}" ]; then
  if ! tb-cli get /prod/hudi/jars/cli/hadoop-common-2.8.2.53.jar "${INTERNAL_HADOOP_JAR}"; then
    echo "Failed to download internal hadoop jar via tb-cli"
    exit 1
  fi
fi

OTHER_JARS=`ls ${DIR}/target/lib/* | grep -v 'hudi-[^/]*jar' | tr '\n' ':'`

HUDI_CLI_CONF_DIR=${DIR}/conf

echo "Running : java -cp ${INTERNAL_HADOOP_JAR}:${HUDI_CLI_CONF_DIR}:${HADOOP_CONF_DIR}:${SPARK_CONF_DIR}:${HOODIE_JAR}:${OTHER_JARS}:${CLIENT_JAR} -DSPARK_CONF_DIR=${SPARK_CONF_DIR} -DHADOOP_CONF_DIR=${HADOOP_CONF_DIR} org.apache.hudi.cli.commands.SparkMain $@"
java -cp ${INTERNAL_HADOOP_JAR}:${HUDI_CLI_CONF_DIR}:${HADOOP_CONF_DIR}:${SPARK_CONF_DIR}:${HOODIE_JAR}:${OTHER_JARS}:${CLIENT_JAR} -DSPARK_CONF_DIR=${SPARK_CONF_DIR} -DHADOOP_CONF_DIR=${HADOOP_CONF_DIR} org.apache.hudi.cli.commands.SparkMain "$@"
