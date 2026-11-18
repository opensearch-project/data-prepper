#!python3

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

import json
import os
import sys

# Bot accounts registered as regular users, so not excluded by the account type.
known_bot_logins = {'opensearch-ci-bot', 'opensearch-trigger-bot'}

authors = []
for author in sys.stdin:
    authors.append(author)


for author in sorted(authors, key=str.lower):
    if author.strip() in known_bot_logins:
        continue

    user = json.loads(os.popen(f"gh api users/{author}").read())

    if user['type'] == 'Bot':
        continue

    if user['name'] != None:
        print(f"* [{user['login']}]({user['html_url']}) -- {user['name']}")
    else:
        print(f"* [{user['login']}]({user['html_url']})")
