#!python3

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

import json
import os
import sys

authors = []
for author in sys.stdin:
    authors.append(author)


for author in sorted(authors, key=str.lower):
    user = json.loads(os.popen(f"gh api users/{author}").read())
    print(f"* [{user['login']}]({user['html_url']}) - {user['name']}")
