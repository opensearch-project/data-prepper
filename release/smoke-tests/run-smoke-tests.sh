#!/bin/bash

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#

set -e

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
    echo -e "-a ARCHITECTURE\tSpecify the architecture (x64 or arm64)."
    echo "--------------------------------------------------------------------------"
}


while getopts "hv:r::o::i::a::" arg; do
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
        a)
            export ARCHITECTURE=$OPTARG
            ;;
        ?)
            echo "Invalid option: -${arg}"
            end_tests 1
            ;;
    esac
done

# Set Docker platform based on architecture
if [ "${ARCHITECTURE}" = "arm64" ]; then
    export DOCKER_DEFAULT_PLATFORM="linux/arm64"
else
    export DOCKER_DEFAULT_PLATFORM="linux/amd64"
fi

export DOCKER_IMAGE="${IMAGE_NAME}:${TAG_NAME}"

echo "Will smoke test image \"${DOCKER_IMAGE}\""

if [ -n "${ARCHITECTURE}" ]; then
    echo "Validating image architecture..."
    ACTUAL_ARCH=$(docker run --rm --entrypoint uname "${DOCKER_IMAGE}" -m)
    EXPECTED_UNAME=$([ "${ARCHITECTURE}" = "arm64" ] && echo "aarch64" || echo "x86_64")
    
    if [ "${ACTUAL_ARCH}" != "${EXPECTED_UNAME}" ]; then
        echo -e "\033[0;31mArchitecture mismatch: expected ${ARCHITECTURE} (${EXPECTED_UNAME}) but image is ${ACTUAL_ARCH}\033[0m"
        end_tests 1
    fi
    echo "Architecture validated: ${ACTUAL_ARCH}"
fi

./gradlew -PendToEndDataPrepperImage=${IMAGE_NAME} -PendToEndDataPrepperTag=${TAG_NAME} :e2e-test:log:basicLogEndToEndTest
sleep 1

./gradlew -PendToEndDataPrepperImage=${IMAGE_NAME} -PendToEndDataPrepperTag=${TAG_NAME} :e2e-test:trace:rawSpanPeerForwarderEndToEndTest
sleep 1

./gradlew -PendToEndDataPrepperImage=${IMAGE_NAME} -PendToEndDataPrepperTag=${TAG_NAME} :e2e-test:trace:serviceMapPeerForwarderEndToEndTest

end_tests 0
