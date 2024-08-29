# Generate Thank You text for the blog

The Data Prepper release blogs include text thanking contributors to the project.
This script makes this easier to accomplish.

You need two tools installed:

* [GitHub CLI](https://cli.github.com/)
* Python 3


Copy the following command and paste into your CLI.
Modify the dates for `since` and `until` to include when work for this release start through when the the release work ended.


```
gh api --paginate '/repos/opensearch-project/data-prepper/commits?since=2024-05-16&until=2024-08-26' | jq -r '.[].author.login' | sort | uniq | ./format-release-thank-you.py
```
