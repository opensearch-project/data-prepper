#!/bin/bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

set -e --trace

REPO_ROOT=`git rev-parse --show-toplevel`
OPENSEARCH_HOST="localhost:9200"
OPENSEARCH_GROK_INDEX="test-grok-index"
OPENSEARCH_OTEL_INDEX="otel-v1-apm-span-000001"

cd ${REPO_ROOT}/release/smoke-tests

function usage() {
    echo ""
    echo "This script is used to build the Docker image. It prepares the files required by the Dockerfile in a temporary directory, then builds and tags the Docker image. Script expects to be run from the project root directory."
    echo "--------------------------------------------------------------------------"
    echo "Usage: $0 [args]"
    echo ""
    echo "Required arguments:"
    echo -e "-v TAG_NAME\tSpecify the image tag name such as '1.2'"
    echo -e "-r REPOSITORY\tSpecify the docker repository name (ex: opensearchstaging or opensearchproject). The tag name will be pointed to '-v' value and 'latest'"
    echo ""
    echo "Optional arguments:"
    echo -e "-h\t\tPrint this message."
    echo "--------------------------------------------------------------------------"
}

function query_hits_gt_zero () {
    local URL=$1
    local SEARCH_RESPONSE=$(curl -s -k -u 'admin:admin' "${URL}")
    local LOG_COUNT=0

    if command -v jq &> /dev/null
    then
        LOG_COUNT=$(jq '.hits.total.value' <<< "${SEARCH_RESPONSE}")
    else
        LOG_COUNT=$(docker run alpine /bin/sh -c "apk -q --no-cache add jq && echo '${SEARCH_RESPONSE}' | jq '.hits.total.value'")
    fi

    if [ $LOG_COUNT -gt 0 ]
    then
        echo "Open Search is receiving logs from Data Prepper"
        echo "Found at least ${LOG_COUNT} hits"
    else
        echo "No hits found with query url ${}"
        exit 1
    fi
}

while getopts "hv:r:" arg; do
    case $arg in
        h)
            usage
            exit 1
            ;;
        v)
            export TAG_NAME=$OPTARG
            ;;
        r)
            export REPOSITORY=$OPTARG
            ;;
        ?)
            echo "Invalid option: -${arg}"
            exit 1
            ;;
    esac
done

echo "Will smoke test image \"${REPOSITORY}/data-prepper:${TAG_NAME}\""

docker-compose up --detach


#Ping Data Prepper until response:
WAITING_FOR_OPENSEARCH=true
while ${WAITING_FOR_OPENSEARCH}
do
    if curl -s -k -u 'admin:admin' 'https://localhost:9200/_cat/indices' > /dev/null && curl -s -k -H "Content-Type: application/json" -d '[{"log": "smoke test log "}]' 'http://localhost:2021/log/ingest' > /dev/null
    then
        WAITING_FOR_OPENSEARCH=false
    else
        echo "Waiting for Data Prepper to start"
    fi
    sleep 1s
done

echo "Data Prepper started!"

query_hits_gt_zero "https://${OPENSEARCH_HOST}/${OPENSEARCH_GROK_INDEX}/_search"
echo "Open Search successfully received logs from Data Prepper!"

query_hits_gt_zero "https://${OPENSEARCH_HOST}/${OPENSEARCH_OTEL_INDEX}/_search?q=PythonService"
echo "Open Search successfully received OTel spans from Data Prepper!"

docker-compose down

cd ${REPO_ROOT}
