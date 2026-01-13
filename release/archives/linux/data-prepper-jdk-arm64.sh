#!/bin/bash

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#

if [[ $# == 0 ]]; then
    echo "Reading pipelines and data-prepper configuration files from Data Prepper home directory."
elif [[ $# == 2 ]]; then
    echo
    echo "Data Prepper now supports reading pipeline and data-prepper configuration files"
    echo "from Data Prepper home directory automatically."
    echo "You can continue to specify paths to configuration files as command line arguments,"
    echo "but that support will be dropped in a future release."
    echo
else
    echo
    echo "Error: Invalid number of arguments. Expected 0 or 2, received $#."
    echo "Put configuration files in Data Prepper home directory and specify no arguments,"
    echo "or specify paths to pipeline and data-prepper configuration files, for example:"
    echo "bin/data-prepper config/example-pipelines.yaml config/example-data-prepper-config.yaml"
    echo
    exit 1
fi

DATA_PREPPER_BIN=$(dirname "$(readlink -f "$0")")
DATA_PREPPER_HOME=`readlink -f "$DATA_PREPPER_BIN/.."`
DATA_PREPPER_CLASSPATH="$DATA_PREPPER_HOME/lib/*"
OPENJDK=$(ls -1 $DATA_PREPPER_HOME/openjdk/ 2>/dev/null)

export JAVA_HOME=$DATA_PREPPER_HOME/openjdk/$OPENJDK
echo "JAVA_HOME is set to $JAVA_HOME"
export PATH=$JAVA_HOME/bin:$PATH

DATA_PREPPER_HOME_OPTS="-Ddata-prepper.dir=$DATA_PREPPER_HOME"
DATA_PREPPER_JAVA_OPTS="-Dfile.encoding=UTF-8 -Dlog4j.configurationFile=$DATA_PREPPER_HOME/config/log4j2-rolling.properties"

if [[ $# == 0 ]]; then
    exec java $DATA_PREPPER_JAVA_OPTS $JAVA_OPTS $DATA_PREPPER_HOME_OPTS -cp "$DATA_PREPPER_CLASSPATH" org.opensearch.dataprepper.DataPrepperExecute
else
    PIPELINES_FILE_LOCATION=$1
    CONFIG_FILE_LOCATION=$2
    exec java $DATA_PREPPER_JAVA_OPTS $JAVA_OPTS $DATA_PREPPER_HOME_OPTS -cp "$DATA_PREPPER_CLASSPATH" org.opensearch.dataprepper.DataPrepperExecute $PIPELINES_FILE_LOCATION $CONFIG_FILE_LOCATION
fi
