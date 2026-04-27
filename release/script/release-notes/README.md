# Generate Release Notes

## AI-Assisted Generation (Recommended)

Use an AI coding assistant to generate categorized, reworded release notes from the GitHub
milestone. The prompt is checked into the repository and can be used with any AI tool.

The prompt file is at: `.claude/commands/generate-release-notes.md`

### Using Claude Code

Claude Code loads this as a slash command automatically:

```
/generate-release-notes v2.15
```

### Using Kiro or Other AI Tools

Ask the AI to read and follow the prompt file, providing the milestone version:

> Read and follow the instructions in `.claude/commands/generate-release-notes.md` for milestone v2.15

### What the AI Does

1. Fetches closed issues and PRs for the milestone using the GitHub CLI
2. Categorizes each item using labels and PR body content (see `label-category-mapping.md`)
3. Rewords items to be active, concise, and user-facing
4. Consolidates related issues/PRs into single entries
5. Excludes internal items (dependency bumps, release prep, license headers)
6. Writes the release notes file to `release/release-notes/`

You should review the output, especially any items the AI flags as uncertain.

### Prerequisites

* [GitHub CLI](https://cli.github.com/) - must be authenticated
* An AI coding assistant (Claude Code, Kiro, etc.)

## Manual Generation (Legacy)

To generate a flat, uncategorized list that requires manual organization:

You need two tools installed:

* [GitHub CLI](https://cli.github.com/)
* Python 3

Run:

```
./generate-initial-release-notes.sh v2.7
```

This will produce a file named `data-prepper.release-notes-version.md`.
You can now use this to help you create the release notes.
These release notes still need some work, but they are nicely formatted to help you get started.
