#!/bin/bash

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
if [[ $# -ne 2 ]]
  then
    echo
    echo "Error: Paths to pipeline and data-prepper configuration files are required. Example:"
    echo "./data-prepper-tar-install.sh config/example-pipelines.yaml config/example-data-prepper-config.yaml"
    echo
    exit 1
fi

PIPELINES_FILE_LOCATION=$1
CONFIG_FILE_LOCATION=$2
DATA_PREPPER_HOME=$(dirname $(realpath $0))
EXECUTABLE_JAR=$(ls -1 $DATA_PREPPER_HOME/bin/*.jar 2>/dev/null)
OPENJDK=$(ls -1 $DATA_PREPPER_HOME/openjdk/ 2>/dev/null)

if [[ -z "$EXECUTABLE_JAR" ]]
then
  echo "Jar file is missing from directory $DATA_PREPPER_HOME/bin"
  exit 1
fi

export JAVA_HOME=$DATA_PREPPER_HOME/openjdk/$OPENJDK
echo "JAVA_HOME is set to $JAVA_HOME"
export PATH=$JAVA_HOME/bin:$PATH

DATA_PREPPER_JAVA_OPTS="-Dlog4j.configurationFile=$DATA_PREPPER_HOME/config/log4j2-rolling.properties"
java $JAVA_OPTS $DATA_PREPPER_JAVA_OPTS -jar $EXECUTABLE_JAR $PIPELINES_FILE_LOCATION $CONFIG_FILE_LOCATION