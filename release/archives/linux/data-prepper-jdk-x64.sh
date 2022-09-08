#!/bin/bash

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

DATA_PREPPER_BIN=$(dirname "$(realpath "$0")")
DATA_PREPPER_HOME=`realpath "$DATA_PREPPER_BIN/.."`
DATA_PREPPER_CLASSPATH="$DATA_PREPPER_HOME/lib/*"
OPENJDK=$(ls -1 $DATA_PREPPER_HOME/openjdk/ 2>/dev/null)

if [[ $# == 2 ]]; then
  PIPELINES_FILE_LOCATION=$1
  CONFIG_FILE_LOCATION=$2
elif [[ $# == 0 ]]; then
  PIPELINES_FILE_LOCATION="$DATA_PREPPER_HOME/pipelines/pipelines.yaml"
  CONFIG_FILE_LOCATION="$DATA_PREPPER_HOME/config/data-prepper-config.yaml"
  echo "Reading pipelines and data-prepper configuration files from Data Prepper home directory."
else
    echo
    echo "Error: Invalid number of arguments. Expected 0 or 2, received $#."
    echo "Use configuration files from Data Prepper home directory and specify no arguments,"
    echo "or specify both paths to pipeline and data-prepper configuration files, for example:"
    echo "./bin/data-prepper config/example-pipelines.yaml config/example-data-prepper-config.yaml"
    echo
    exit 1
fi

export JAVA_HOME=$DATA_PREPPER_HOME/openjdk/$OPENJDK
echo "JAVA_HOME is set to $JAVA_HOME"
export PATH=$JAVA_HOME/bin:$PATH

DATA_PREPPER_JAVA_OPTS="-Dlog4j.configurationFile=$DATA_PREPPER_HOME/config/log4j2-rolling.properties"
java $JAVA_OPTS $DATA_PREPPER_JAVA_OPTS -cp "$DATA_PREPPER_CLASSPATH" org.opensearch.dataprepper.DataPrepperExecute $PIPELINES_FILE_LOCATION $CONFIG_FILE_LOCATION
