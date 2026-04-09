# Label to Category Mapping for Release Notes

This file defines how GitHub labels map to release note categories.
It is referenced by the release notes generation prompt.

## Categories

The release notes use these fixed categories, in this order:

1. **Breaking Changes**
2. **Features**
3. **Enhancements**
4. **Bug Fixes**
5. **Security**
6. **Maintenance**

## Label Mapping Rules

### Breaking Changes
- `breaking change`

### Bug Fixes
- `bug`

### Security
- `security fix`
- `Mend: dependency security vulnerability`

### Maintenance
- `maintenance`
- `dependencies`
- `ci`
- `github_actions`
- `documentation`

### Features vs Enhancements

Both Features and Enhancements often use the `enhancement` label. Use these rules to distinguish:

**Features** (entirely new capabilities):
- Introduces a new source, processor, sink, or buffer plugin
- Adds a wholly new integration (e.g. new AWS service support)
- Adds a new expression function or operator
- The issue/PR title or body describes something that did not exist before

**Enhancements** (improvements to existing capabilities):
- Adds a configuration option to an existing plugin
- Improves performance of an existing feature
- Extends an existing plugin to support a new format, codec, or protocol
- The `ease-of-use` or `performance` labels are present

When the `enhancement` label alone is present, read the PR/issue body to determine which category applies.

## Items to Exclude

Do not include items in the release notes for:
- Release preparation PRs (e.g. "Prepare release X.Y.Z", version bumps for the release)
- Changelog and release notes PRs themselves
- Individual dependabot/Mend dependency bump PRs (summarize these under Maintenance if there are notable upgrades, otherwise omit)
- PRs that only update license headers, fix typos in code comments, or fix test flakiness with no user-facing impact
- PRs from bots that are purely automated housekeeping

## Items to Consolidate

- When an issue and one or more PRs address the same feature/fix, produce a single release note line referencing the issue (preferred) or the primary PR
- When multiple PRs implement parts of the same feature, consolidate into one line
