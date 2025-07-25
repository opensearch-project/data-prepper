#!/bin/bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

export IMAGE_NAME="opensearch-data-prepper"

function end_tests () {
    local EXIT_CODE=$1

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
    echo "This script runs specific Data Prepper end-to-end tests to smoke test a release. Script expects to be run from the project root directory."
    echo "--------------------------------------------------------------------------"
    echo "Usage: $0 [args]"
    echo ""
    echo "Required arguments:"
    echo -e "-v TAG_NAME\tSpecify the image tag name such as '1.2'"
    echo ""
    echo "Optional arguments:"
    echo -e "-h\t\tPrint this message."
    echo -e "-i IMAGE_NAME\tOverride the docker image name name (ex: opensearch-data-prepper or data-prepper)."
    echo "--------------------------------------------------------------------------"
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
        i)
            export IMAGE_NAME=$OPTARG
            ;;
        ?)
            echo "Invalid option: -${arg}"
            end_tests 1
            ;;
    esac
done

export DOCKER_IMAGE="${IMAGE_NAME}:${TAG_NAME}"

# Check the Docker image before running
if ! docker inspect --type=image "${DOCKER_IMAGE}" > /dev/null
then
    echo "--------------------------------------------------------------------------"
    echo "Unable to find image \"${DOCKER_IMAGE}\" are you sure it exists?"
    end_tests 1
fi

echo "Will smoke test image \"${DOCKER_IMAGE}\""

./gradlew -PendToEndDataPrepperImage=${IMAGE_NAME} -PendToEndDataPrepperTag=${TAG_NAME} :e2e-test:log:basicLogEndToEndTest
sleep 1

./gradlew -PendToEndDataPrepperImage=${IMAGE_NAME} -PendToEndDataPrepperTag=${TAG_NAME} :e2e-test:trace:rawSpanPeerForwarderEndToEndTest
sleep 1

./gradlew -PendToEndDataPrepperImage=${IMAGE_NAME} -PendToEndDataPrepperTag=${TAG_NAME} :e2e-test:trace:serviceMapPeerForwarderEndToEndTest

end_tests 0
