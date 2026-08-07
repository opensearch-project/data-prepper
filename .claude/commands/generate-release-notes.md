---
description: Generate release notes for Data Prepper version $ARGUMENTS.
allowed-tools:
  - Bash(gh issue list *)
  - Bash(gh pr list *)
  - Bash(gh pr view *)
  - Bash(gh api repos/opensearch-project/project-website/pulls/*)
  - Bash(curl -sL https://raw.githubusercontent.com/opensearch-project/*)
  - Read(release/script/release-notes/**)
  - Read(release/release-notes/**)
  - Write(release/release-notes/**)
  - AskUserQuestion
  - WebFetch
---

Generate release notes for Data Prepper version $ARGUMENTS.

## Overview

You are generating user-facing release notes for the OpenSearch Data Prepper project.
The milestone to use is: $ARGUMENTS

You will gather data from GitHub, categorize each item, reword it for clarity, and produce
a release notes file following the project's established format.

## Step 1: Gather Data

Run these commands to fetch all closed issues and PRs for the milestone.
Request the fields: number, url, title, labels, body.

```
gh issue list --repo opensearch-project/data-prepper --search 'milestone:$ARGUMENTS' --state closed --json number,url,title,labels,body --limit 200
```

```
gh pr list --repo opensearch-project/data-prepper --search 'milestone:$ARGUMENTS' --state closed --json number,url,title,labels,body --limit 200
```

Collect all results. Note which items are issues vs PRs.

## Step 2: Read the Label-to-Category Mapping

Read the file `release/script/release-notes/label-category-mapping.md` for the full rules
on how labels map to categories, how to distinguish Features from Enhancements, what to
exclude, and what to consolidate.

## Step 3: Read Example Release Notes for Style

Read 2-3 of the most recent release notes files in `release/release-notes/`. Use only
files matching `data-prepper.release-notes-*.md` -- ignore `data-prepper.change-log-*.md`
files, which are a different format. Study the tone, format, and level of detail.
Key style observations:

- Each item is a single bullet point: `* Description ([#N](url))`
- Descriptions are concise, active, and user-facing
- They describe what changed from the user's perspective, not implementation details
- Multiple related links can appear: `([#N1](url1), [#N2](url2))`
- Security items use the format: `* CVE-XXXX-XXXXX - library version ([#N](pr-url))`
  - Lead with the CVE identifier(s), then the library name and the version that fixes it
  - Link to the PR that performed the upgrade, not the vulnerability issue
  - If multiple CVEs are fixed by the same upgrade PR, combine them on one line:
    `* CVE-XXXX-XXXXX, CVE-YYYY-YYYYY - library version ([#N](pr-url))`
  - Search for the fixing PR by looking up the CVE or library name in closed PRs
- The date format is YYYY-MM-DD
- Empty categories are kept in the output (the heading with no items below it)

## Step 4: Find the Release Blog

The OpenSearch project publishes a release blog post for each Data Prepper minor release
in the `opensearch-project/project-website` repository. The blog is written by maintainers
and reflects an editorial judgment of which changes matter most. It is an authoritative
cross-check: every feature called out in the blog must appear in the release notes, and the
blog's ordering tells us which items the maintainers consider most significant.

**Skip this entire step for patch releases.** Blogs are only created for minor releases
(those ending in `.0`, such as `2.16.0`). Patch releases (such as `2.15.1`) do not get a
blog, so there is nothing to find or cross-check. When the version is a patch release, skip
Step 4 outright and ignore the blog-related guidance in Step 7 (prominence ordering) and
Step 8 (blog cross-check).

1. **Try to find the blog PR automatically.** The blog post lives at
   `_posts/YYYY-MM-DD-Data-Prepper-VERSION.md` (for example
   `_posts/2026-06-16-Data-Prepper-2.16.md`). Search open and recently merged PRs:

   ```
   gh pr list --repo opensearch-project/project-website --search 'Data Prepper VERSION in:title' --state all --json number,title,url --limit 20
   ```

   You can also look directly for the post file in a candidate PR:

   ```
   gh api repos/opensearch-project/project-website/pulls/<PR_NUMBER>/files --jq '.[] | select(.filename | test("Data-Prepper")) | .raw_url'
   ```

   Then fetch the raw content of that file with `curl -sL <raw_url>` (or WebFetch).

2. **If you cannot confidently identify the blog PR, ask the user.** Use AskUserQuestion to
   request the release blog PR URL (or confirm that no blog exists yet). Do not guess at a
   blog that may belong to a different version.

3. **If no blog exists yet**, note this to the user and continue — the blog cross-check in
   Step 7 and the prominence ordering in Step 6 are simply skipped.

4. **Once you have the blog, extract two things:**
   - The list of every feature, enhancement, and notable change it describes, with the
     issue/PR number where given. This becomes the completeness checklist for Step 7.
   - The order in which the blog presents items. Items with their own blog section, listed
     earlier, are the most significant. This drives the ordering in Step 6.

## Step 5: Categorize Each Item

For each issue/PR:

1. **Check exclusion rules first.** Skip release prep PRs, changelog PRs, release notes PRs,
   individual dependency bumps, license header updates, and bot housekeeping PRs.

2. **Apply label-based categorization.** Use the mapping rules to assign a category.

3. **Confirm by reading the body.** If the labels suggest one category but the body clearly
   indicates another, override the label-based assignment. Pay special attention to:
   - Distinguishing Features (new capability) from Enhancements (improving existing)
   - Items labeled `enhancement` that are actually bug fixes
   - Items with no relevant labels -- categorize based on the body content

4. **Consolidate related items.** When an issue and its implementing PR(s) are both in the
   milestone, produce one line. Prefer linking to the issue. If a PR has no corresponding
   issue, link to the PR.

## Step 6: Reword Each Item

Rewrite each item to be:

- **Direct and concise**: "Support X" not "Add support for X", "Added support for X",
  or "Support for X was added". Lead with the verb, skip filler words.
- **User-facing**: Describe the impact, not the implementation. "Support compressed files
  in the file source" not "Add GzipInputStream wrapper to FileSource class"
- **Concise**: One sentence, no trailing periods
- **Specific**: Name the plugin or component when relevant. "Support partition keys in the
  OTel metrics source with persistent buffers" not "Support partition keys"

Do NOT use the prefix tags like "feat:", "fix:", "refactor:" from commit messages.

## Step 7: Produce the Output

Write the release notes file to:
`release/release-notes/data-prepper.release-notes-VERSION.md`

where VERSION is derived from the milestone (e.g. milestone `v2.15` produces version `2.15.0`
if it's a minor release).

Use today's date in the header.

**Ordering within each category.** Within a category, list the items the release blog (Step 4)
considers most significant first. Anything that has its own section in the blog, or appears
earlier in it, goes higher in the matching release notes category. Items not mentioned in the
blog follow, in a sensible order. When there is no blog — including every patch release — order
by significance to the user.

Format:

```
## YYYY-MM-DD Version M.m.p

---

### Breaking Changes

* Item ([#N](url))

### Features

* Item ([#N](url))

### Enhancements

* Item ([#N](url))

### Bug Fixes

* Item ([#N](url))

### Security

* Item ([#N](url))

### Maintenance

* Item ([#N](url))

```

Keep all six category headings even if a category has no items.

## Step 8: Review

After writing the file, read it back and check:
- No duplicate items
- No excluded items leaked through
- Items are in the correct categories
- Wording is consistent in style
- All links are correct

**Cross-check against the release blog (from Step 4).** Skip this cross-check for patch
releases, which have no blog. If a blog exists:
- **Completeness:** every feature or notable change the blog describes must appear somewhere
  in the release notes. If the blog highlights something the notes omit, add it. Treat the
  blog as the source of truth for what shipped.
- **Naming accuracy:** verify plugin, processor, and config option names against the blog and,
  when in doubt, against the code in the repository. The blog is written and reviewed by
  maintainers, so a mismatch usually means the release notes are wrong.
- **Prominence:** confirm the most significant blog items are ordered near the top of their
  category, per Step 6.
- **Discrepancies:** call out to the user anything that appears in the blog but not in the
  milestone (or vice versa) so they can reconcile it.

Present the final file to the user. Note any items where categorization was uncertain, and
report the result of the blog cross-check — including anything you added, renamed, or could
not reconcile — so they can review those specifically.
