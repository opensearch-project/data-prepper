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


## Generating with Claude Code

If you use Claude Code, the `/generate-blog-thank-you` command automates this whole process,
including figuring out the `since` and `until` dates for you. It derives the date window from
the release branches: `since` is when the previous release branch was cut from `main` and
`until` is when the current release branch is cut (or today, if that branch does not exist yet).

```
/generate-blog-thank-you 2.16.0
```
