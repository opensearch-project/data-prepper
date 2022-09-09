#!/bin/bash

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
if [[ $# -ne 0 ]]
  then
    echo
    echo "Invalid number of arguments. Expected 0, received $#"
    echo
    exit 1
fi

MIN_REQ_JAVA_VERSION=11
MIN_REQ_OPENJDK_VERSION=11
DATA_PREPPER_BIN=$(dirname "$(realpath "$0")")
DATA_PREPPER_HOME=`realpath "$DATA_PREPPER_BIN/.."`
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
    java_version=$("$_java" -version 2>&1 | awk -F '"' '/version/ {print $2}' | sed 's/\(.*\..*\)\..*/\1/g')
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

DATA_PREPPER_JAVA_OPTS="-Dlog4j.configurationFile=$DATA_PREPPER_HOME/config/log4j2-rolling.properties"
java $JAVA_OPTS $DATA_PREPPER_JAVA_OPTS -cp "$DATA_PREPPER_CLASSPATH" org.opensearch.dataprepper.DataPrepperExecute
