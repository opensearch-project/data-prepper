#!/bin/bash

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

set -e

REPO_ROOT=`git rev-parse --show-toplevel`

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

while getopts ":hv:r:" arg; do
    case $arg in
        h)
            usage
            exit 1
            ;;
        v)
            TAG_NAME=$OPTARG
            ;;
        r)
            REPOSITORY=$OPTARG
            ;;
        ?)
            echo "Invalid option: -${arg}"
            exit 1
            ;;
    esac
done

# Validate the required parameters to present
if [ -z "$TAG_NAME" ] || [ -z "$REPOSITORY" ]; then
  echo "You must specify '-v TAG_NAME', '-r REPOSITORY'."
  usage
  exit 1
fi

# Warning docker desktop
if !(docker version > /dev/null 2>&1); then
    echo -e "Could not run 'docker version'. You MUST have Docker Desktop."
    exit 1
fi

if [ ! -f "$REPO_ROOT/gradlew" ]; then
    echo "Could not find $REPO_ROOT/gradlew"
    usage
    exit 1
fi

cd $REPO_ROOT
./gradlew clean :release:docker:docker -Prelease -Pversion=$TAG_NAME
docker tag opensearch-data-prepper:$TAG_NAME $REPOSITORY/data-prepper:$TAG_NAME
docker tag opensearch-data-prepper:$TAG_NAME $REPOSITORY/data-prepper:latest
docker push $REPOSITORY/data-prepper:$TAG_NAME
docker push $REPOSITORY/data-prepper:latest
