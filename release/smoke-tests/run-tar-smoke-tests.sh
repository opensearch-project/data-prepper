#!/bin/bash

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

set -ex

function run_smoke_test() {
    export FROM_IMAGE_NAME=$1
    export FROM_IMAGE_TAG=$2
    export NAME=$3

    export BUILD_NAME="${NAME}-${DATA_PREPPER_VERSION}-linux-x64"
    export TAR_FILE="${BUILD_NAME}.tar.gz"

    echo "Smoke testing tar ${TAR_FILE}"
    echo "Using Docker Image ${FROM_IMAGE_NAME}:${FROM_IMAGE_TAG} as base image"
    eval "${DOCKER_FILE_DIR}/build.sh"

    local CURRENT_DIR
    CURRENT_DIR=$(pwd)

    eval "${REPO_DIR}/release/smoke-tests/run-smoke-tests.sh -i ${NAME} -v ${DATA_PREPPER_VERSION}"

    echo echo "Completed smoke testing tar ${TAR_FILE}"

    cd "${CURRENT_DIR}"
}

export REPO_DIR
REPO_DIR=$(pwd)

export DOCKER_FILE_DIR="${REPO_DIR}/release/smoke-tests/data-prepper"
export FROM_IMAGE_NAME="openjdk"
export FROM_IMAGE_TAG="14"
#export NAME="opensearch-data-prepper"
export DATA_PREPPER_VERSION="1.3.0-SNAPSHOT"

#export NAME="opensearch-data-prepper-jdk"

run_smoke_test "${FROM_IMAGE_NAME}" "${FROM_IMAGE_TAG}" "opensearch-data-prepper"
#run_smoke_test "${FROM_IMAGE_NAME}" "${FROM_IMAGE_TAG}" "opensearch-data-prepper-jdk"
