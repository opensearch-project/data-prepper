#!/bin/bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

set -e

export REPO_DIR
REPO_DIR=$(pwd)

export DOCKER_FILE_DIR="${REPO_DIR}/release/smoke-tests/data-prepper-tar"

function is_defined() {
    if [ -z "$1" ]
    then
        false
    else
        true
    fi
}

function usage() {
    echo ""
    echo "This script is used to test Data Prepper tarball files. For more information see https://github.com/opensearch-project/data-prepper/blob/main/release/README.md#running-smoke-tests-on-tarball-files"
    echo "--------------------------------------------------------------------------"
    echo "Usage: $0 [args]"
    echo ""
    echo "Required arguments:"
    echo -e "-v DATA_PREPPER_VERSION\tSpecify the Data Prepper build version to test such as '1.3.0-SNAPSHOT'."
    echo ""
    echo "Optional arguments:"
    echo -e "-b BUCKET\tSpecify an AWS bucket name of bucket containing tarball files to be smoke tested."
    echo -e "-d TAR_DIR\tSpecify local directory containing tarball files to be smoke tested."
    echo -e "          \tExample: (from repo root folder)"
    echo -e "-h\t\tPrint this message."
    echo "--------------------------------------------------------------------------"
    echo "Examples, run from repo root directory"
    echo -e "\t\"./release/smoke-tests/run-tarball-files-smoke-tests.sh -v 1.3.0-SNAPSHOT -d release/archives/linux/build/distributions\""
    echo ""
    echo "--------------------------------------------------------------------------"
    exit 0
}

function missing_tar_source() {
    echo -e "Invalid arguments received by run-tarball-files-smoke-tests.sh"
    echo -e "One of the following sets of options must be specified"
    echo -e "\t-b BUCKET_NAME -b BUILD_NUMBER"
    echo -e "\t-d TAR_DIR"
    echo -e "Use \"run-tarball-files-smoke-tests.sh -h\" for a list of valid script arguments."
    exit 1
}

function get_tar() {
    local DOCKER_FILE_DIR=${1}
    local TAR_FILE=${2}

    if is_defined "${3}" && is_defined "${4}"
    then
        local BUCKET_NAME=${3}
        local BUILD_NUMBER=${4}

        local S3_KEY="${DATA_PREPPER_VERSION}/${BUILD_NUMBER}/archive/${TAR_FILE}"

        echo -e "Smoke testing tar file s3://${BUCKET_NAME}/${S3_KEY}"

        aws s3 cp "s3://${BUCKET_NAME}/${S3_KEY}" "${DOCKER_FILE_DIR}/${TAR_FILE}"
    elif is_defined "${3}"
    then
        local TAR_DIR=${3}

        if [ -f "${TAR_DIR}/${TAR_FILE}" ]; then
            echo -e "Smoke testing tar file ${TAR_DIR}/${TAR_FILE}"

            cp "${TAR_DIR}/${TAR_FILE}" "${DOCKER_FILE_DIR}/${TAR_FILE}"
        else
            echo -e "Argument \"-d ${TAR_DIR}\" specified but file ${TAR_DIR}/${TAR_FILE} not found."
            echo -e "Files available in ${TAR_DIR}:"
            ls "${TAR_DIR}"
            exit 1
        fi
    else
        echo -e "Invalid arguments received by get_tar function."
        echo -e "Valid options:"
        echo -e "\tget_tar DOCKER_FILE_DIR TAR_FILE TAR_DIR"
        echo -e "\tget_tar DOCKER_FILE_DIR TAR_FILE BUCKET_NAME BUILD_NUMBER"
        echo -e ""
        echo -e "Options received:"
        echo -e "\tget_tar $*"
        echo -e ""
        echo -e "Caused by:"
        missing_tar_source
    fi
}

function run_smoke_test() {
    export FROM_IMAGE_NAME=$1
    export FROM_IMAGE_TAG=$2
    export NAME=$3

    export BUILD_NAME="${NAME}-${DATA_PREPPER_VERSION}-linux-x64"
    export TAR_FILE="${BUILD_NAME}.tar.gz"

    if is_defined "${4}" && is_defined "${5}"
    then
        local BUCKET_NAME=${4}
        local BUILD_NUMBER=${5}
        get_tar "${DOCKER_FILE_DIR}" "${TAR_FILE}" "${BUCKET_NAME}" "${BUILD_NUMBER}"
    elif is_defined "${4}"
    then
        local TAR_DIR=${4}
        get_tar "${DOCKER_FILE_DIR}" "${TAR_FILE}" "${TAR_DIR}"
    else
        missing_tar_source
    fi

    echo "Smoke testing tar ${TAR_FILE}"
    echo "Using Docker Image ${FROM_IMAGE_NAME}:${FROM_IMAGE_TAG} as base image"
    eval "${DOCKER_FILE_DIR}/build.sh"

    local CURRENT_DIR
    CURRENT_DIR=$(pwd)

    eval "${REPO_DIR}/release/smoke-tests/run-smoke-tests.sh -i ${NAME} -v ${DATA_PREPPER_VERSION}"

    echo echo "Completed smoke testing tar ${TAR_FILE}"

    cd "${CURRENT_DIR}" || exit
}

while getopts "b:d:hn:v:" arg; do
    case $arg in
        b)
            export BUCKET_NAME=$OPTARG
            ;;
        d)
            export TAR_DIR=$OPTARG
            ;;
        h)
            usage
            ;;
        n)
            export BUILD_NUMBER=$OPTARG
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

if ! is_defined "${DATA_PREPPER_VERSION}"
then
    echo -e "Argument \"-v <Data Prepper version>\" required but not provided."
    echo -e "Use \"run-tarball-files-smoke-tests.sh -h\" for a list of valid script arguments."
    exit 1
fi

run_smoke_test "openjdk" "8" "opensearch-data-prepper" "${BUCKET_NAME:-${TAR_DIR}}" "${BUILD_NUMBER}"
run_smoke_test "openjdk" "11" "opensearch-data-prepper" "${BUCKET_NAME:-${TAR_DIR}}" "${BUILD_NUMBER}"
run_smoke_test "openjdk" "17" "opensearch-data-prepper" "${BUCKET_NAME:-${TAR_DIR}}" "${BUILD_NUMBER}"
run_smoke_test "ubuntu" "latest" "opensearch-data-prepper-jdk" "${BUCKET_NAME:-${TAR_DIR}}" "${BUILD_NUMBER}"
