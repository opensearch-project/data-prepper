#!/bin/bash

# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0

set -e

export REPO_DIR
REPO_DIR=$(pwd)

export DOCKER_FILE_DIR="${REPO_DIR}/release/smoke-tests/data-prepper-tar"

function invalid_args() {
    echo -e "Invalid arguments provided"
    echo -e "Use \"run-tarball-files-smoke-tests.sh -h\" for a list of valid script arguments."
    exit 1
}

function is_defined() {
    if [ -z "$1" ]
    then
        false
    else
        true
    fi
}

function usage() {
    echo -e ""
    echo -e "This script is used to test Data Prepper tarball files. For more information see https://github.com/opensearch-project/data-prepper/blob/main/release/README.md#running-smoke-tests-on-tarball-files"
    echo -e "--------------------------------------------------------------------------"
    echo -e "Usage: $0 [args]"
    echo -e ""
    echo -e "Required arguments:"
    echo -e "-v DATA_PREPPER_VERSION\tSpecify the Data Prepper build version to test such as '1.3.0-SNAPSHOT'."
    echo -e ""
    echo -e "Only one of the following argument sets can be used:"
    echo -e "Smoke test local file arguments:"
    echo -e "-d TAR_DIR\tSpecify local directory containing tarball files to be smoke tested."
    echo -e ""
    echo -e "Smoke test s3 object arguments:"
    echo -e "-b BUCKET\tSpecify an AWS bucket name of bucket containing tarball files to be smoke tested."
    echo -e "-n BUILD_NUMBER\tSpecify the github build number. Uses with BUCKET to create s3 path for tarball files to be smoke tested."
    echo -e ""
    echo -e "Smoke test file from url arguments:"
    echo -e "-n BUILD_NUMBER\tSpecify the github build number. Uses with BUCKET to create s3 path for tarball files to be smoke tested."
    echo -e "-u BASE_URL\t"
    echo -e "--------------------------------------------------------------------------"
    echo -e "Examples, run from repo root directory"
    echo -e "\t\"./release/smoke-tests/run-tarball-files-smoke-tests.sh -v 1.3.0-SNAPSHOT -d release/archives/linux/build/distributions\""
    echo -e "\t\"./release/smoke-tests/run-tarball-files-smoke-tests.sh -v 1.3.0-SNAPSHOT -b staging-bucket -n 1\""
    echo -e "\t\"./release/smoke-tests/run-tarball-files-smoke-tests.sh -v 1.3.0-SNAPSHOT -u https://staging.opensearch.org -n 1\""
    echo -e ""
    echo -e "--------------------------------------------------------------------------"
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

function print_testing_tar() {
    echo -e "\033[0;35mSmoke Testing\033[0m ${1}"
}

function get_local_tar() {
    local DOCKER_FILE_DIR=${1}
    local TAR_FILE=${2}
    local TAR_DIR=${3}
    if [ -f "${TAR_DIR}/${TAR_FILE}" ]; then
        print_testing_tar "$(pwd)/${TAR_DIR}/${TAR_FILE}"

        cp "${TAR_DIR}/${TAR_FILE}" "${DOCKER_FILE_DIR}/${TAR_FILE}"
    else
        echo -e "Argument \"-d ${TAR_DIR}\" specified but file ${TAR_DIR}/${TAR_FILE} not found."
        echo -e "Files available in ${TAR_DIR}:"
        ls "${TAR_DIR}"
        exit 1
    fi
}

function get_s3_tar() {
    local DOCKER_FILE_DIR=${1}
    local TAR_FILE=${2}
    local BUILD_NUMBER=${3}
    local DATA_PREPPER_VERSION=${4}
    local BUCKET_NAME=${5}

    local S3_KEY="${DATA_PREPPER_VERSION}/${BUILD_NUMBER}/archive/${TAR_FILE}"

    print_testing_tar "s3://${BUCKET_NAME}/${S3_KEY}"

    aws s3 cp "s3://${BUCKET_NAME}/${S3_KEY}" "${DOCKER_FILE_DIR}/${TAR_FILE}"
}

function get_url_tar() {
    local DOCKER_FILE_DIR=${1}
    local TAR_FILE=${2}
    local BUILD_NUMBER=${3}
    local DATA_PREPPER_VERSION=${4}
    local BASE_URL=${5}

    local FILE_URL="${BASE_URL}/${DATA_PREPPER_VERSION}/${BUILD_NUMBER}/archive/${TAR_FILE}"
    print_testing_tar "${FILE_URL}"

    curl "${FILE_URL}" --output "${DOCKER_FILE_DIR}/${TAR_FILE}"
}

function run_smoke_test() {
    export BUILD_NAME="${TAR_NAME}-${DATA_PREPPER_VERSION}-linux-x64"
    export TAR_FILE="${BUILD_NAME}.tar.gz"

    case $TAR_SOURCE_TYPE in
        "local file") get_local_tar "${DOCKER_FILE_DIR}" "${TAR_FILE}" "${TAR_DIR}";;
        "s3 object") get_s3_tar "${DOCKER_FILE_DIR}" "${TAR_FILE}" "${BUILD_NUMBER}" "${DATA_PREPPER_VERSION}" "${BUCKET_NAME}";;
        "url file") get_url_tar "${DOCKER_FILE_DIR}" "${TAR_FILE}" "${BUILD_NUMBER}" "${DATA_PREPPER_VERSION}" "${BASE_URL}";;
        ?) invalid_args;;
    esac

    if [ ! -f "${DOCKER_FILE_DIR}/${TAR_FILE}" ]
    then
        echo -e "Unable to retrieve tarball file"
        exit 1
    fi

    export SMOKE_IMAGE_NAME="${TAR_NAME}-smoke-test"
    echo "Using Docker Image ${FROM_IMAGE} as base image"
    eval "${DOCKER_FILE_DIR}/build.sh"

    local CURRENT_DIR
    CURRENT_DIR=$(pwd)

    eval "${REPO_DIR}/release/smoke-tests/run-smoke-tests.sh -i ${SMOKE_IMAGE_NAME} -v ${DATA_PREPPER_VERSION}"

    echo echo "Completed smoke testing tar ${TAR_FILE}"

    cd "${CURRENT_DIR}" || exit
}

while getopts "b:d:hi:n:t:u:v:" arg; do
    case $arg in
        b) export BUCKET_NAME=$OPTARG;;
        d) export TAR_DIR=$OPTARG;;
        h) usage;;
        i) export FROM_IMAGE=$OPTARG;;
        n) export BUILD_NUMBER=$OPTARG;;
        t) export TAR_NAME=$OPTARG;;
        u) export BASE_URL=$OPTARG;;
        v) export DATA_PREPPER_VERSION=$OPTARG;;
        ?) invalid_args;;
    esac
done

if ! is_defined "${DATA_PREPPER_VERSION}"
then
    echo -e "Argument \"-v <Data Prepper version>\" required but not provided."
    echo -e "Use \"run-tarball-files-smoke-tests.sh -h\" for a list of valid script arguments."
    exit 1
fi

if is_defined "${TAR_DIR}"
then
    if is_defined "${BUCKET_NAME}" || is_defined "${BASE_URL}" || is_defined "${BUILD_NUMBER}"
    then
        invalid_args
    else
        export TAR_SOURCE_TYPE="local file"
    fi
elif is_defined "${BUCKET_NAME}" && is_defined "${BUILD_NUMBER}"
then
    if is_defined "${TAR_DIR}" || is_defined "${BASE_URL}"
    then
        invalid_args
    else
        export TAR_SOURCE_TYPE="s3 object"
    fi
elif is_defined "${BASE_URL}" && is_defined "${BUILD_NUMBER}"
then
    if is_defined "${TAR_DIR}" || is_defined "${BUCKET_NAME}"
    then
        invalid_args
    else
        export TAR_SOURCE_TYPE="url file"
    fi
else
    invalid_args
fi

if ! is_defined "${FROM_IMAGE}"
then
    export FROM_IMAGE="eclipse-temurin:11"
fi

if ! is_defined "${TAR_NAME}"
then
    export TAR_NAME="opensearch-data-prepper"
fi


echo -e "Using tarball source ${TAR_SOURCE_TYPE}"

run_smoke_test
