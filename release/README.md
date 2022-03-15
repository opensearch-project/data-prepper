# Release Project for Data Prepper

The projects in this directory include different mechanisms for releasing Data Prepper. Each subdirectory in here provides a different
mechanism for the release process. See the `README.md` in each sub-directory for specifics on what it does.

The [`release-notes`](release-notes) sub-directory is different from the other sub-directories.
It is not a project and instead holds release notes and change logs for Data Prepper releases.

## Performing a Release

This section outlines how to perform a Data Prepper release using GitHub Actions and the OpenSearch build infrastructure.
The audience for this section are Data Prepper maintainers.

### Prerequisites

You should have created a RELEASE Issue in the [opensearch-build](https://github.com/opensearch-project/opensearch-build) project.

### Building the Data Prepper Artifacts

To run the release, go to the [Release Artifacts](https://github.com/opensearch-project/data-prepper/actions/workflows/release.yml)
GitHub Action.

Select the "Run workflow" option from the GitHub Actions UI. GitHub will prompt you to select a branch with the "Use workflow from" option. Select
the release branch which you are releasing for. You may select `main` for testing.

Press the "Run workflow" button.

GitHub Actions will perform the release process. Please take note of the workflow run number. You will find this in the text "Release Artifacts #<worflow run number>"

Wait for the job to complete successfully.

### Promotion to Production

Once you have a completed build, you may update the release Issue you created in the opensearch-build project. Provide the following:

* `VERSION` - The version you are releasing (e.g. `1.3.0`)
* `DATA_PREPPER_BUILD_NUMBER` - The workflow run number from the GitHub Action which build the artifacts. Do not include the `#`. e.g. `3`
