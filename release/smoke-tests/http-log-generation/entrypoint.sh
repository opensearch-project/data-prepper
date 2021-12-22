#!/bin/bash

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

DATA_PREPPER_HOST="data-prepper"
DATA_PREPPER_PORT="2021"
OPENSEARCH_HOST="node-0.example.com:9200"

echo "Waiting for startup"

WAITING_FOR_OPENSEARCH=true

while ${WAITING_FOR_OPENSEARCH}
do
    if curl -s -k -H "Content-Type: application/json" -d '[{"log": "test log"}]' "http://${DATA_PREPPER_HOST}:${DATA_PREPPER_PORT}/log/ingest" > /dev/null
    then
        WAITING_FOR_OPENSEARCH=false
    else
        echo "Waiting for Data Prepper to start at http://${DATA_PREPPER_HOST}:${DATA_PREPPER_PORT}"
    fi
    sleep 1s
done

echo "Data Prepper started!"

set -x

echo "Starting to send cURL requests to ${DATA_PREPPER_HOST}"

i=0
while [ $i -lt 10 ]
do
    curl -k \
        -H "Content-Type: application/json" \
        -d "[{\"log\": \"smoke test log ${i}\"}]" \
        "http://${DATA_PREPPER_HOST}:${DATA_PREPPER_PORT}/log/ingest"
    i=$((i+1))
done
