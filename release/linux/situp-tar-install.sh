#!/bin/bash

# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
if [[ $# -eq 0 ]]
  then
    echo "Path to the configuration file is required"
    exit 1
fi

CONFIG_FILE_LOCATION=$1
MIN_REQ_JAVA_VERSION=1.8
MIN_REQ_OPENJDK_VERSION=8
SITUP_HOME=$(dirname $(realpath $0))
EXECUTABLE_JAR=$(ls -1 $SITUP_HOME/bin/*.jar 2>/dev/null)

#check if java is installed
if type -p java; then
    _java=java
elif [[ -n "$JAVA_HOME" ]] && [[ -x "$JAVA_HOME/bin/java" ]];  then
    _java="$JAVA_HOME/bin/java"
else
    echo "java is required for executing situp, Using $SITUP_HOME/openjdk for jdk"
    export JAVA_HOME=$SITUP_HOME/openjdk/jdk-14/
    echo "JAVA_HOME is set to $JAVA_HOME"
    export PATH=$JAVA_HOME/bin:$PATH
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

chmod 755 $SITUP_HOME/bin/*.jar
SITUP_JAVA_OPTS="-Dlog4j.configurationFile=$SITUP_HOME/config/log4j.properties"
java $JAVA_OPTS $SITUP_JAVA_OPTS -jar $EXECUTABLE_JAR $CONFIG_FILE_LOCATION