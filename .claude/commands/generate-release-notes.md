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

## Step 4: Categorize Each Item

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

## Step 5: Reword Each Item

Rewrite each item to be:

- **Direct and concise**: "Support X" not "Add support for X", "Added support for X",
  or "Support for X was added". Lead with the verb, skip filler words.
- **User-facing**: Describe the impact, not the implementation. "Support compressed files
  in the file source" not "Add GzipInputStream wrapper to FileSource class"
- **Concise**: One sentence, no trailing periods
- **Specific**: Name the plugin or component when relevant. "Support partition keys in the
  OTel metrics source with persistent buffers" not "Support partition keys"

Do NOT use the prefix tags like "feat:", "fix:", "refactor:" from commit messages.

## Step 6: Produce the Output

Write the release notes file to:
`release/release-notes/data-prepper.release-notes-VERSION.md`

where VERSION is derived from the milestone (e.g. milestone `v2.15` produces version `2.15.0`
if it's a minor release).

Use today's date in the header.

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

## Step 7: Review

After writing the file, read it back and check:
- No duplicate items
- No excluded items leaked through
- Items are in the correct categories
- Wording is consistent in style
- All links are correct

Present the final file to the user and note any items where categorization was uncertain
so they can review those specifically.
