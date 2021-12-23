#!/bin/bash

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

java -javaagent:/app/opentelemetry-javaagent-all.jar -jar /app/spring-boot-application.jar &

until [[ $(curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8087) != 000 ]]; do
  echo "Waiting for analytics-service to be ready"
  sleep 1
done

python3 databaseService.py &

until [[ $(curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8083) != 000 ]]; do
  echo "Waiting for databaseService to be ready"
  sleep 1
done

python3 orderService.py &

until [[ $(curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8088) != 000 ]]; do
  echo "Waiting for orderService to be ready"
  sleep 1
done

python3 inventoryService.py &

until [[ $(curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8082) != 000 ]]; do
  echo "Waiting for inventoryService to be ready"
  sleep 1
done

python3 paymentService.py &

until [[ $(curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8084) != 000 ]]; do
  echo "Waiting for paymentService to be ready"
  sleep 1
done

python3 recommendationService.py &

until [[ $(curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8086) != 000 ]]; do
  echo "Waiting for recommendationService to be ready"
  sleep 1
done

python3 authenticationService.py &

until [[ $(curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8085) != 000 ]]; do
  echo "Waiting for authenticationService to be ready"
  sleep 1
done

python3 client.py &

until [[ $(curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8089) != 000 ]]; do
  echo "Waiting for client to be ready"
  sleep 1
done

# Naive check runs checks once a minute to see if either of the processes exited. 
# The container exits with an error if it detects that either of the processes has exited.
# Otherwise it loops forever, waking up every 60 seconds

while sleep 60; do
  NUM_JOBS=$(jobs|grep -c Running)
  if [ $NUM_JOBS -lt 8 ]; then
    echo "One of the services or client dashboard has already stopped."
    exit 1
  fi
done