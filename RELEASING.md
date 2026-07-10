# Releasing

This document outlines the process for releasing OpenSearch Data Prepper.
It is a guide for maintainers of the OpenSearch Data Prepper project to release a new version.

## Overview

This document has four broad categories of steps to follow:

1. [Pre-release coordination](#pre-release-coordination)
2. [Release setup](#release-setup)
3. [Performing a release](#performing-a-release)
4. [Post-release](#post-release)


## <a name="pre-release-coordination">Pre-release coordination</a>

Before performing a release, there are a couple of coordination steps to start early since they involve other teams and take time to complete.

### Start the release blog process

We typically write a blog post to announce a minor release.
Note that we do not write blog posts for patch releases.
Starting this early gives authors time to write and review the post before the release.

Steps:

1. Identify the authors for the blog post.
2. Create a blog post issue using the [blog post template](https://github.com/opensearch-project/project-website/issues/new?template=blog_post.yml).
3. Work on the blog post in a fork.
4. Create a PR with the blog post.

### Coordinate for the downloads page

This step is applicable for all releases.

We should notify the maintainers of the [project-website](https://github.com/opensearch-project/project-website) of our intention to release.
This repository is where the downloads page resides.
This coordination is to prepare for updating the downloads page, which we will update later in the release process.

Steps:

1. [Create a new issue in the project-website repository](https://github.com/opensearch-project/project-website/issues/new/choose).
2. Choose _Feature Request_.
3. Provide them with the:
    1. Project: Data Prepper
    2. Version you are releasing
    3. Release date

Here is an example issue: https://github.com/opensearch-project/project-website/issues/4156


## <a name="release-setup">Release setup</a>

These steps can begin a few days before the release.

### Branch setup

OpenSearch Data Prepper uses a release branch for releasing.
The [Developer Guide](docs/developer_guide.md#backporting) discusses this in detail.

The repository has a release branch for a major/minor version.
Patch versions will continue on the same branch.
For example, OpenSearch Data Prepper `2.6.0` was released from the `2.6` branch.
Additionally, OpenSearch Data Prepper `2.6.1` and `2.6.2` were also released from the `2.6` branch.

If you are creating a new major/minor release, then you will need to create the release branch.
The OpenSearch project restricts branch creation, so you must request the branch by creating a [GitHub Request](https://github.com/opensearch-project/.github/issues/new?template=GITHUB_REQUEST_TEMPLATE.yaml).
These requests typically take 1 to 2 business days to complete, so plan accordingly.

Provide:
* The repository: `data-prepper`
* The branch name - It is in the format of `{major}.{minor}`. e.g. `2.6`
* The source branch to create it from: `main`

Here is an example request: https://github.com/opensearch-project/.github/issues/562.

Create a backport label for the release branch.

Steps:
1. Go to the [labels page here](https://github.com/opensearch-project/data-prepper/labels).
2. Select `New label`
3. Name the branch `backport {major}.{minor}`. For example, `backport 2.6`

Once the branch is created, you will need to backport other changes into the branch if you want them in the release.

#### Update on the main branch

The OpenSearch Data Prepper version is defined in the [`gradle.properties`](https://github.com/opensearch-project/data-prepper/blob/main/gradle.properties) file.
After creating a new branch, we must also bump the version for the `main` branch to the next version.

If you have just created a release branch, you should also create a PR on the `main` branch to bump the version.

For example, if you have started the `2.7` branch, you will need to update the `main` branch from `2.7.0-SNAPSHOT` to `2.8.0-SNAPSHOT`.

### Prepare release branch

For any release, you must prepare the release branch.
This is applicable for new major/minor releases and patch releases.

Steps:
* Go the [Prepare Release Branch](https://github.com/opensearch-project/data-prepper/actions/workflows/release-prepare-branch.yml) action.
* Select `Run workflow`
* Choose the branch you are releasing. e.g. `2.15`
* Press `Run workflow`
* Wait for a new PR to be created
* Spot check the PR, approve and merge


This action will create a PR that updates files as necessary

* `gradle.properties`
* `THIRD-PARTY`

Approve this and merge it before proceeding with the release.

### Prepare release notes

Prepare release notes and check them into the `main` branch in the [`release-notes` directory](https://github.com/opensearch-project/data-prepper/tree/main/release/release-notes).
The format for the release notes file is `data-prepper.release-notes.{major}.{minor}.{patch}.md`.

The best way to generate release notes is using Claude using the `generate-release-notes` command.

```
/generate-release-notes v2.16
```

See the [README](release/script/release-notes/README.md) for the script for detailed instructions.

Once the release notes are ready, create a PR to merge them into `main`.
Also tag this with the `backport {major}.{minor}` to create a PR that you can merge into your release branch.


## <a name="performing-a-release">Performing a release</a>

This section outlines how to perform a OpenSearch Data Prepper release using GitHub Actions and the OpenSearch build infrastructure.
The audience for this section are OpenSearch Data Prepper maintainers.

### Request the tag

Create a [GitHub Request](https://github.com/opensearch-project/.github/issues/new?template=GITHUB_REQUEST_TEMPLATE.yaml) for the tag.
Provide:
* The repository: `data-prepper`
* The tag name - It should be the full version: `{major}.{minor}.{patch}`. e.g. `2.16.0`
* The branch - It is in the format of `{major}.{minor}`. e.g. `2.16`
* The commit SHA of the commit to tag.
* You can also provide a link to the GitHub commit for extra clarity on what to tag.

Here is an example request: https://github.com/opensearch-project/.github/issues/564.


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
This includes building the artifacts, testing, creating a GitHub pre-release, and promoting to production

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

### OpenSearch CI server build

After two maintainers approve the GitHub issue, the [OpenSearch CI server](https://build.ci.opensearch.org/) will start a build.
This is a Jenkins server that promotes our releases. 
You can check the promotion status by checking the [release-data-prepper](https://build.ci.opensearch.org/job/release-data-prepper/) job.

You can also check the Maven artifacts after the promote stage completes by viewing the [AWS Sonatype repository](https://aws.oss.sonatype.org/content/repositories/releases/org/opensearch/dataprepper/).

### Request downloads page update

Coordinate with the [project-website](https://github.com/opensearch-project/project-website) team to have them update the downloads page.

### Publish the blog post

Coordinate on the blog post issue you created earlier to let them know that you are ready for them to publish.

## <a name="post-release">Post release</a>

After the release, there are a few other steps to clean up the repository.

### Approve & merge changelog

Each release creates a changelog of all commits between releases.
This is fully automated by the release build.
During the release GitHub Action, the `opensearch-ci-bot` will create a PR with the changelog.
Review it, approve, and merge.


### Update the release notes

The release process will have created a pre-release for the new version.
The next step is to update the pre-release with the release notes created before the release.

Steps:
* Go to the [releases page](https://github.com/opensearch-project/data-prepper/releases)
* Find the new pre-release. It should be at the top.
* Replace the auto-generated release notes with the release notes created previous to the release.
* Under the _Release label_ heading, change the label from _Pre-release_ to _Latest_, then update the release.

### Community engagement

We update the community in a few places.
For minor releases, take these steps after the blog is published so that you can link to it.

* Blog posts for major/minor releases. This should already be done.
* Creating a new announcement on the [Data Prepper discussions page](https://github.com/opensearch-project/data-prepper/discussions). Be sure to pin it as well.
* Posts in `#ingest` and `#data-prepper` channels of OpenSearch Slack.

### Close the GitHub milestone

Steps:
* Go to the [milestones](https://github.com/opensearch-project/data-prepper/milestones) page.
* Find the milestone for the release.
* Make sure there are no issues. If there are any triage them by closing, or changing the milestone.
* Click the "Close" button


## Further details

For more details on the release build, or to setup your own GitHub repository, see [release/README.md](release/README.md).
