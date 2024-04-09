#!/bin/sh

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

milestone_version=$1

cat release-notes-template.md > data-prepper.release-notes-version.md

echo '### ISSUES' >> data-prepper.release-notes-version.md
gh issue list --search 'milestone:'"${milestone_version}" --state closed --json number,url,title,labels --limit 100 | python3 format-release-notes.py >> data-prepper.release-notes-version.md

echo '### PULL REQUESTS' >> data-prepper.release-notes-version.md
gh pr list --search 'milestone:'"${milestone_version}" --state closed --json number,url,title,labels --limit 100 | python3 format-release-notes.py >> data-prepper.release-notes-version.md
