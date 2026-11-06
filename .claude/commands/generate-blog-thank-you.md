---
description: Generate the blog "thank you" contributor list for release $ARGUMENTS, auto-computing the date window.
allowed-tools:
  - Bash(git branch *)
  - Bash(git merge-base *)
  - Bash(git show *)
  - Bash(git log *)
  - Bash(git fetch *)
  - Bash(gh api *)
  - Read(release/script/blog/**)
---

Generate the contributor "thank you" text for the Data Prepper release blog for version $ARGUMENTS.

## Overview

The release blog thanks everyone who contributed during the development of a release. The
manual process is documented in `release/script/blog/README.md`: run a `gh api` commits query
bounded by a `since` and `until` date, then pipe the authors through
`release/script/blog/format-release-thank-you.py`.

The only part that requires human judgment in that process is picking the two dates. This
command automates that. The work for a release spans the time from when the **previous**
release branch was cut up to when the **current** release branch is cut.

Data Prepper uses a release branch named `{major}.{minor}` for each minor/major release (see
`RELEASING.md`). The point at which a branch was cut from `main` is the merge-base of that
branch with `main`.

The target version is: $ARGUMENTS

## Step 1: Determine the current and previous release branches

From the target version $ARGUMENTS, derive the current release branch name `{major}.{minor}`
(e.g. version `2.16.0` -> branch `2.16`). Drop the patch component; patch releases ship from
the same branch as their minor release.

Fetch the latest remote refs so branch and merge-base data are current:

```
git fetch origin --quiet
```

List the release branches to find the one immediately preceding the current version:

```
git branch -a --list '*[0-9].[0-9]*'
```

Identify the previous release branch — the highest `{major}.{minor}` that sorts before the
current version (e.g. for `2.16` the previous is `2.15`; for `3.0` it would be the latest `2.x`).
Sort numerically, not lexically (`2.15` > `2.9`).

## Step 2: Compute the `since` date (previous branch cut)

The `since` date is when the previous release branch was cut from `main` — the commit date of
the merge-base between that branch and `main`.

```
git merge-base main origin/PREVIOUS_BRANCH
```

```
git show -s --format=%cI MERGE_BASE_COMMIT
```

Use the date portion (YYYY-MM-DD) as `since`.

## Step 3: Compute the `until` date (current branch cut)

If the current release branch **already exists**, compute its cut date the same way:

```
git merge-base main origin/CURRENT_BRANCH
git show -s --format=%cI MERGE_BASE_COMMIT
```

If the current release branch does **not exist yet** (the branch is being cut now, as part of
this release), use **today's date** as `until` — the release work ends when the branch is cut.

Determine branch existence by checking whether `origin/CURRENT_BRANCH` appeared in the Step 1
listing.

## Step 4: State the window

Before running the query, print the computed window for the user to confirm, e.g.:

```
Release: 2.16.0  (branch 2.16)
since: 2026-04-02  (branch 2.15 cut from main)
until: 2026-06-30  (branch 2.16 not yet cut — using today)
```

## Step 5: Generate the thank-you list

Run the documented query with the computed dates, piping through the formatting script. Run
this from the `release/script/blog` directory so the script path resolves:

```
gh api --paginate '/repos/opensearch-project/data-prepper/commits?since=SINCE&until=UNTIL' | jq -r '.[].author.login' | sort | uniq | ./format-release-thank-you.py
```

The script filters out bot accounts and formats each contributor as a markdown link, using
their GitHub display name when available.

## Step 6: Present the output

Present the formatted markdown list to the user, along with the date window used so they can
adjust the dates and re-run if the boundaries need tweaking.
