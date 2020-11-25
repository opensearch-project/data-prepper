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
SITUP_HOME=$(cd "$(dirname "$0")"; pwd)
EXECUTABLE_JAR=$(ls -1 $SITUP_HOME/bin/*.jar 2>/dev/null)
OPENJDK=$(ls -1 $SITUP_HOME/openjdk/ 2>/dev/null)

if [[ -z "$EXECUTABLE_JAR" ]]
then
  echo "Jar file is missing from directory $SITUP_HOME/bin"
  exit 1
fi

export JAVA_HOME=$SITUP_HOME/openjdk/$OPENJDK/Contents/Home
export PATH=$JAVA_HOME/bin:$PATH
SITUP_JAVA_OPTS="-Dlog4j.configurationFile=$SITUP_HOME/config/log4j.properties"
$JAVA_HOME/bin/java $JAVA_OPTS $SITUP_JAVA_OPTS -jar $EXECUTABLE_JAR $CONFIG_FILE_LOCATION