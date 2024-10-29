# Releasing

This document outlines the process for releasing OpenSearch Data Prepper.
It is a guide for maintainers of the OpenSearch Data Prepper project to release a new version.

## Overview

This document has three broad categories of steps to follow:

1. [Release setup](#release-setup)
2. [Performing a release](#performing-a-release)
3. [Post-release](#post-release)


## <a name="release-setup">Release setup</a>

### Branch setup

OpenSearch Data Prepper uses a release branch for releasing.
The [Developer Guide](docs/developer_guide.md#backporting) discusses this in detail.

The repository has a release branch for a major/minor version.
Patch versions will continue on the same branch.
For example, OpenSearch Data Prepper `2.6.0` was released from the `2.6` branch.
Additionally, OpenSearch Data Prepper `2.6.1` and `2.6.2` were also released from the `2.6` branch.

If you are creating a new major/minor release, then you will need to create the release branch.
Use GitHub to create a new branch.

Steps:
1. Go to the [branches page here](https://github.com/opensearch-project/data-prepper/branches).
2. Select `New branch`
3. Name the branch `{major}.{minor}`. For example, `2.6`.
4. Select `main` as the source.

Create a backport label for the release branch.

Steps:
1. Go to the [labels page here](https://github.com/opensearch-project/data-prepper/labels).
2. Select `New label`
3. Name the branch `backport {major}.minor`. For example, `backport 2.6`

### Update versions

The OpenSearch Data Prepper version is defined in the [`gradle.properties`](https://github.com/opensearch-project/data-prepper/blob/main/gradle.properties)  file.
We must update this whenever we create a new release.
We will need two PRs to update it.

#### Update on release branch

For the current release branch, update the version to the new version.
You may either need to update by removing the `-SNAPSHOT` or by updating the patch version.

For example, when we released `2.6.2`, the property was set as `version=2.6.2`.
You can see the [exact commit here](https://github.com/opensearch-project/data-prepper/blob/2.6.2/gradle.properties#L8).

Create a PR that targets the release branch with the change.
Do not create a PR to `main` for this.

#### Update on the main branch

If you have just created a release branch, you should also create a PR on the `main` branch to bump the version.

For example, if you have started the `2.7` branch, you will need to update the `main` branch from `2.6.0-SNAPSHOT` to `2.7.0-SNAPSHOT`.

#### Update the version of DataPrepperVersion

If you have just created a release branch, you should also create a PR on the `main` branch to bump the version in `DataPrepperVersion`.

Steps:
1. Modify the `DataPrepperVersion` class to update `CURRENT_VERSION` to the next version.
2. Create a PR targeting `main`

Note: This step can be automated through [#4877](https://github.com/opensearch-project/data-prepper/issues/4877).

### Update the THIRD-PARTY file

We should update the `THIRD-PARTY` file for every release.
OpenSearch Data Prepper has a GitHub action that will generate this and create a PR with the updated file.

Steps:
* Go the [Third Party Generate action](https://github.com/opensearch-project/data-prepper/actions/workflows/third-party-generate.yml)
* Select `Run workflow`
* Choose the branch you are releasing. e.g. `2.6`
* Press `Run workflow`
* Wait for a new PR to be created
* Spot check the PR, approve and merge


### Prepare release notes

Prepare release notes and check them into the `main` branch in the [`release-notes` directory](https://github.com/opensearch-project/data-prepper/tree/main/release/release-notes).
The format for the release notes file is `data-prepper.release-notes.{major}.{minor}.{patch}.md`.

You can use a script to help you generate these.
See the [README](release/script/release-notes/README.md) for the script for instructions.

Once the release notes are ready, create a PR to merge them into `main`.
Also tag this with the `backport {major}.{minor}` to create a PR that you can merge into your release branch.

### Create changelog

You can create a changelog using [git-release-notes](https://github.com/ariatemplates/git-release-notes).

```
git fetch upstream
git switch {major}.{minor}
git fetch upstream --tags
git pull
git-release-notes {previousMajor}.{previousMinor}.{previousPatch}..HEAD markdown > release/release-notes/data-prepper.change-log-{major}.{minor}.{patch}.md
git switch main
```

Once the change log ready, create a PR to merge it into `main`.
Also tag this with the `backport {major}.{minor}` to create a PR that you can merge into your release branch.



## <a name="performing-a-release">Performing a release</a>

This section outlines how to perform a OpenSearch Data Prepper release using GitHub Actions and the OpenSearch build infrastructure.
The audience for this section are OpenSearch Data Prepper maintainers.

### Start the release OpenSearch Data Prepper action

To run the release, go to the [Release Artifacts](https://github.com/opensearch-project/data-prepper/actions/workflows/release.yml)
GitHub Action.

Select the "Run workflow" option from the GitHub Actions UI. GitHub will prompt you with some options.

#### Use workflow for

Select the release branch which you are releasing for.
Typically, this will be a branch such as `2.6`.
However, you may select `main` for testing.

#### Whether to create major tag of docker image or not.

This will create a tag such as `2` which points to this version

All releases have a full version tag. For example, `2.6.0`.
The latest release on a major series can also have a tag applied such as `2`.
Check this option if you are releasing the latest version withing that series of major versions.
This value can be true for old major series as well such as the 1.x series.

#### Whether to create latest tag of docker image or not.

This will update the `latest` tag to point to this version.
You should set this when releasing the latest version, but not patches to old versions.

#### Run

Press the "Run workflow" button.

GitHub Actions will perform the release process.
This includes building the artifacts, testing, drafting a GitHub release, and promoting to production

### Approve issue

The release build will create a new GitHub issue requesting to release the project.
This needs two maintainers to approve.
To approve, load [OpenSearch Data Prepper issues](https://github.com/opensearch-project/data-prepper/issues).
Look for and open a new issue starting with _Manual approval required for workflow_.
Verify that the metadata looks correct and that we want to release.
Add a new comment on the issue with the word _approve_ or _approved_ in it.
(See the issue for all allowed words)
Once approved by two maintainers, the release build will be promoted to production.

You can also deny a release by using _deny_ or _denied_ in the comment.

**NOTE** The smoke tests currently report a build failure, even when they succeed. Thus, you need to manually verify the output.

### Further details

For more details on the release build, or to setup your own GitHub repository, see [release/README.md](release/README.md).



## <a name="post-release">Post release</a>

After the release, there are a few other steps to clean up the repository.

### Update the release notes

The release process will have created a draft release for the new version.
The next step is to update the draft release with the release notes created before the release.

Steps:
* Go to the [releases page](https://github.com/opensearch-project/data-prepper/releases)
* Find the new draft release. It should be at the top.
* Replace the auto-generated release notes with the release notes created previous to the release.
* Release it

### Close the GitHub milestone

Steps:
* Go to the [milestones](https://github.com/opensearch-project/data-prepper/milestones) page.
* Find the milestone for the release.
* Make sure there are no issues. If there are any triage them by closing, or changing the milestone.
* Click the "Close" button
