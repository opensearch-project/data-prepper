# Releasing

This document outlines the process for releasing Data Prepper.

## Release prerequisites

Be sure you have:

* Created a release branch.
* Updated the version in the release branch.
* Updated the THIRD-PARTY file.
* Created the release notes file
* Created the changelog file

## Performing a release

This section outlines how to perform a Data Prepper release using GitHub Actions and the OpenSearch build infrastructure.
The audience for this section are Data Prepper maintainers.

### Start the release Data Prepper action

To run the release, go to the [Release Artifacts](https://github.com/opensearch-project/data-prepper/actions/workflows/release.yml)
GitHub Action.

Select the "Run workflow" option from the GitHub Actions UI. GitHub will prompt you with some options.

#### Use workflow for

Select the release branch which you are releasing for.
Typically, this will be a branch such as `2.4`.
However, you may select `main` for testing.

#### Whether to create major tag of docker image or not.

This will create a tag such as `2` which points to this version

All releases have a full version tag. For example, `2.4.0`.
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
To approve, load [Data Prepper issues](https://github.com/opensearch-project/data-prepper/issues).
Look for and open a new issue starting with _Manual approval required for workflow_.
Verify that the metadata looks correct and that we want to release.
Add a new comment on the issue with the word _approve_ or _approved_ in it.
(See the issue for all allowed words)
Once approved by two maintainers, the release build will be promoted to production.

You can also deny a release by using _deny_ or _denied_ in the comment.

**NOTE** The smoke tests currently report a build failure, even when they succeed. Thus, you need to manually verify the output.

### Further details

For more details on the release build, or to setup your own GitHub repository, see [release/README.md](release/README.md).
