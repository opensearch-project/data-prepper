#!python3

#
# Copyright OpenSearch Contributors
# SPDX-License-Identifier: Apache-2.0
#

import json
import sys

json_input = sys.stdin.read()

issues = json.loads(json_input)

for issue in issues:

  print(f'* {issue["title"]} ([#{issue["number"]}]({issue["url"]}))')
