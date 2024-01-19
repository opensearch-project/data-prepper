#!/bin/bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

set -e

export IMAGE_NAME="opensearch-data-prepper"
REPO_ROOT=$(git rev-parse --show-toplevel)
export OPENSEARCH_VERSION="1.3.6"
OPENSEARCH_HOST="localhost:9200"
OPENSEARCH_GROK_INDEX="test-grok-index"
OPENSEARCH_OTEL_INDEX="otel-v1-apm-span-000001"

spin[0]="-"
spin[1]="\\"
spin[2]="|"
spin[3]="/"

cd "${REPO_ROOT}/release/smoke-tests"

function end_tests () {
    local EXIT_CODE=$1
    docker-compose down
    cd "${REPO_ROOT}"

    if [ "${EXIT_CODE}" -ne 0 ]
    then
        echo -e "\033[0;31mSmoke tests failed\033[0m"
    else
        echo -e "\033[0;32mSmoke tests passed\033[0m"
    fi
    exit "${EXIT_CODE}"
}

function usage() {
    echo ""
    echo "This script is used to build the Docker image. It prepares the files required by the Dockerfile in a temporary directory, then builds and tags the Docker image. Script expects to be run from the project root directory."
    echo "--------------------------------------------------------------------------"
    echo "Usage: $0 [args]"
    echo ""
    echo "Required arguments:"
    echo -e "-v TAG_NAME\tSpecify the image tag name such as '1.2'"
    echo ""
    echo "Optional arguments:"
    echo -e "-h\t\tPrint this message."
    echo -e "-r REPOSITORY\tSpecify the docker repository name (ex: opensearchstaging or opensearchproject). The tag name will be pointed to '-v' value and 'latest'"
    echo -e "-i IMAGE_NAME\tOverride the docker image name name (ex: opensearch-data-prepper or data-prepper)."
    echo -e "-o OPENSEARCH_VERSION\tOverride the default Open Search version used in smoke tests"
    echo "--------------------------------------------------------------------------"
}

function query_hits_gt_zero () {
    local URL=$1
    local SEARCH_RESPONSE
    SEARCH_RESPONSE=$(curl -s -k -u 'admin:myStrongPassword123!' "${URL}")
    local LOG_COUNT=0

    if command -v jq &> /dev/null
    then
        LOG_COUNT=$(jq '.hits.total.value' <<< "${SEARCH_RESPONSE}")
    else
        LOG_COUNT=$(docker run alpine /bin/sh -c "apk -q --no-cache add jq && echo '${SEARCH_RESPONSE}' | jq '.hits.total.value'")
    fi

    if [ "${LOG_COUNT}" -gt 0 ]
    then
        echo "Open Search is receiving logs from Data Prepper"
        echo "Found at least ${LOG_COUNT} hits"
        echo -e "\033[0;32mTest passed\033[0m"
    else
        echo "No hits found with query url ${URL}"
        echo "Smoke test failed"
        end_tests 1
    fi
}

while getopts "hv:r::o::i::" arg; do
    case $arg in
        h)
            usage
            end_tests 1
            ;;
        v)
            export TAG_NAME=$OPTARG
            ;;
        r)
            export REPOSITORY=$OPTARG
            ;;
        o)
            export OPENSEARCH_VERSION=$OPTARG
            ;;
        i)
            export IMAGE_NAME=$OPTARG
            ;;
        ?)
            echo "Invalid option: -${arg}"
            end_tests 1
            ;;
    esac
done

if [ -z ${REPOSITORY+x} ]
then
    # REPOSITORY not defined
    export DOCKER_IMAGE="${IMAGE_NAME}:${TAG_NAME}"
else
    # REPOSITORY is defined
    export DOCKER_IMAGE="${REPOSITORY}/${IMAGE_NAME}:${TAG_NAME}"
    if ! docker pull "${DOCKER_IMAGE}" > /dev/null 2> /dev/null
    then
        echo "--------------------------------------------------------------------------"
        echo "Unable to pull image \"${DOCKER_IMAGE}\" are you sure it exists?"
        end_tests 1
    fi
fi

if ! docker inspect --type=image "${DOCKER_IMAGE}" > /dev/null
then
    echo "--------------------------------------------------------------------------"
    echo "Unable to find image \"${DOCKER_IMAGE}\" are you sure it exists?"
    end_tests 1
fi

echo "Will smoke test image \"${DOCKER_IMAGE}\""

docker-compose down > /dev/null 2> /dev/null

if ! docker-compose up --detach
then
    echo "--------------------------------------------------------------------------"
    echo "Failed to start all docker-compose services"
    end_tests 1
fi

#Ping Data Prepper until response:
WAITING_FOR_DATAPREPPER=true
echo -n "Waiting for Data Prepper to start  "
while ${WAITING_FOR_DATAPREPPER}
do
    if curl -s -k -u 'admin:myStrongPassword123!' 'https://localhost:9200/_cat/indices' > /dev/null && curl -s -k -H "Content-Type: application/json" -d '[{"log": "smoke test log "}]' 'http://localhost:2021/log/ingest' > /dev/null
    then
        WAITING_FOR_DATAPREPPER=false
    else
        for i in "${spin[@]}"
        do
            echo -ne "\b$i"
            sleep 0.1
        done
    fi
done

echo -e "\b "
echo "Data Prepper started!"

sleep 30

echo "Ready to begin smoke tests. Running cURL commands."
echo ""

echo -e "\033[0;33mTest:\033[0m Verify logs received via HTTP were processed"
query_hits_gt_zero "https://${OPENSEARCH_HOST}/${OPENSEARCH_GROK_INDEX}/_search"
echo "Open Search successfully received logs from Data Prepper!"
echo ""

echo -e "\033[0;33mTest:\033[0m Verify metrics received via grpc were processed"
query_hits_gt_zero "https://${OPENSEARCH_HOST}/${OPENSEARCH_OTEL_INDEX}/_search?q=PythonService"
echo "Open Search successfully received OTel spans from Data Prepper!"
echo ""

end_tests 0
