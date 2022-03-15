#!/bin/bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

#set -e

export REPO_DIR
REPO_DIR=$(pwd)

export DOCKER_FILE_DIR="${REPO_DIR}/release/smoke-tests/data-prepper"

function usage() {
    echo ""
    echo "This script is used to test Data Prepper tarball files. For more information see https://github.com/opensearch-project/data-prepper/blob/main/release/README.md#running-smoke-tests-on-tarball-files"
    echo "--------------------------------------------------------------------------"
    echo "Usage: $0 [args]"
    echo ""
    echo "Required arguments:"
    echo -e "-v DATA_PREPPER_VERSION\tSpecify the Data Prepper build version to test such as '1.3.0-SNAPSHOT'"
    echo ""
    echo "Optional arguments:"
    echo -e "-h\t\tPrint this message."
    echo "--------------------------------------------------------------------------"
}

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

    cd "${CURRENT_DIR}" || exit
}

while getopts "hv:" arg; do
    case $arg in
        h)
            usage
            exit 1
            ;;
        v)
            export DATA_PREPPER_VERSION=$OPTARG
            ;;
        ?)
            echo -e "Invalid option: -${arg}"
            echo -e "Use \"run-tarball-files-smoke-tests.sh -h\" for a list of valid script arguments."
            exit 1
            ;;
    esac
done

if [ -z ${DATA_PREPPER_VERSION+x} ]
then
    echo -e "Argument \"-v <Data Prepper version>\" required but not provided."
    echo -e "Use \"run-tarball-files-smoke-tests.sh -h\" for a list of valid script arguments."
    exit 1
fi

run_smoke_test "openjdk" "8" "opensearch-data-prepper"
run_smoke_test "openjdk" "14" "opensearch-data-prepper"
run_smoke_test "openjdk" "17" "opensearch-data-prepper"
run_smoke_test "ubuntu" "latest" "opensearch-data-prepper-jdk"
