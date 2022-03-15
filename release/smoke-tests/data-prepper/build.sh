#!/bin/bash

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

set -e

function copy_if_needed() {
    local SOURCE_DIR=$1
    local TARGET_DIR=$2
    local FILE_NAME=$3

    echo "Checking if file exists: ${TARGET_DIR}/${FILE_NAME}"

    if [ -f "${TARGET_DIR}/${FILE_NAME}" ]; then
        echo "File already exists, skipping copy"
    else
        echo "File missing, starting copy"
        cp "${SOURCE_DIR}/${FILE_NAME}" "${TARGET_DIR}"
    fi
}


copy_if_needed "${REPO_DIR}/release/archives/linux/build/distributions" "${DOCKER_FILE_DIR}" "${TAR_FILE}"

docker build \
    --build-arg BUILD_NAME="${BUILD_NAME}" \
    --build-arg DATA_PREPPER_VERSION="${DATA_PREPPER_VERSION}" \
    --build-arg DOCKER_FILE_DIR="${DOCKER_FILE_DIR}" \
    --build-arg FROM_IMAGE_NAME="${FROM_IMAGE_NAME}" \
    --build-arg FROM_IMAGE_TAG="${FROM_IMAGE_TAG}" \
    --build-arg TAR_FILE="${TAR_FILE}" \
    -t "${NAME}":"${DATA_PREPPER_VERSION}" \
    "${DOCKER_FILE_DIR}"

rm -f "${DOCKER_FILE_DIR}/${TAR_FILE}"

echo "Image created: \"${NAME}:${DATA_PREPPER_VERSION}\""
