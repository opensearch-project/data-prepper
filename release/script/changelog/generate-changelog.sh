#!/bin/bash

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#

# Generates a changelog for a Data Prepper release using git-release-notes.
#
# Usage: generate-changelog.sh <version>
#
# The previous version is determined automatically from existing git tags.
# Requires git-release-notes to be installed (npm install -g git-release-notes).

set -e

VERSION=$1

if [ -z "$VERSION" ]; then
  echo "Usage: $0 <version>"
  echo "  <version>  The release version (e.g. 2.15.0)"
  exit 1
fi

PREVIOUS_VERSION=$(git tag --sort=-version:refname | grep -E '^[0-9]+\.[0-9]+\.[0-9]+$' | awk -v ver="$VERSION" 'found {print; exit} $0 == ver {found=1}')

if [ -z "$PREVIOUS_VERSION" ]; then
  echo "Error: Could not determine the previous version from git tags."
  echo "Make sure the tag '$VERSION' exists and there is an earlier release tag."
  exit 1
fi

echo "Generating changelog for ${PREVIOUS_VERSION}..${VERSION}"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"
OUTPUT_FILE="${REPO_ROOT}/release/release-notes/data-prepper.change-log-${VERSION}.md"

git-release-notes "${PREVIOUS_VERSION}..${VERSION}" markdown > "$OUTPUT_FILE"

echo "Changelog written to $OUTPUT_FILE"
