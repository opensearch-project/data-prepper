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

DATA_PREPPER_BIN=$(dirname "$(realpath "$0")")
DATA_PREPPER_HOME=`realpath "$DATA_PREPPER_BIN/.."`
DATA_PREPPER_CLASSPATH="$DATA_PREPPER_HOME/lib/*"
OPENJDK=$(ls -1 $DATA_PREPPER_HOME/openjdk/ 2>/dev/null)

export JAVA_HOME=$DATA_PREPPER_HOME/openjdk/$OPENJDK
echo "JAVA_HOME is set to $JAVA_HOME"
export PATH=$JAVA_HOME/bin:$PATH

DATA_PREPPER_JAVA_OPTS="-Dlog4j.configurationFile=$DATA_PREPPER_HOME/config/log4j2-rolling.properties"
java $JAVA_OPTS $DATA_PREPPER_JAVA_OPTS -cp "$DATA_PREPPER_CLASSPATH" org.opensearch.dataprepper.DataPrepperExecute
