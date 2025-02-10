#!/bin/bash

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
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

MIN_REQ_JAVA_VERSION=11
MIN_REQ_OPENJDK_VERSION=11
DATA_PREPPER_BIN=$(dirname "$(readlink -f "$0")")
DATA_PREPPER_HOME=`readlink -f "$DATA_PREPPER_BIN/.."`
DATA_PREPPER_CLASSPATH="$DATA_PREPPER_HOME/lib/*"

#check if java is installed
if type -p java; then
    _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    _java="$JAVA_HOME/bin/java"
else
    echo "java is required for executing data prepper, consider downloading data prepper tar with jdk"
    exit 1
fi

if [[ "$_java" ]]
then
    java_type=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $1}')
    java_version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}' | sed 's/\([0-9]*\.[0-9]*\)\..*/\1/g')
    echo "Found $java_type of $java_version"
    if [[ $java_type == *"openjdk"* ]]
    then
        if (( $(echo "$java_version < $MIN_REQ_OPENJDK_VERSION" | bc -l) ))
        then
            echo "Minimum required for $java_type is $MIN_REQ_OPENJDK_VERSION"
            exit 1
        fi
    else
        if (( $(echo "$java_version < $MIN_REQ_JAVA_VERSION" | bc -l) ))
        then
            echo "Minimum required for $java_type is $MIN_REQ_JAVA_VERSION"
            exit 1
        fi
    fi
fi

DATA_PREPPER_HOME_OPTS="-Ddata-prepper.dir=$DATA_PREPPER_HOME"
DATA_PREPPER_JAVA_OPTS="-Dfile.encoding=UTF-8 -Dlog4j.configurationFile=$DATA_PREPPER_HOME/config/log4j2-rolling.properties"

if [[ $# == 0 ]]; then
    exec java $DATA_PREPPER_JAVA_OPTS $JAVA_OPTS $DATA_PREPPER_HOME_OPTS -cp "$DATA_PREPPER_CLASSPATH" org.opensearch.dataprepper.DataPrepperExecute
else
    PIPELINES_FILE_LOCATION=$1
    CONFIG_FILE_LOCATION=$2
    exec java $DATA_PREPPER_JAVA_OPTS $JAVA_OPTS $DATA_PREPPER_HOME_OPTS -cp "$DATA_PREPPER_CLASSPATH" org.opensearch.dataprepper.DataPrepperExecute $PIPELINES_FILE_LOCATION $CONFIG_FILE_LOCATION
fi
