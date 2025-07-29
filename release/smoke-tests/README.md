# Smoke Tests

This directory contains smoke tests for Data Prepper. Data Prepper smoke tests run some of the end-to-end tests.
They are intended to run a small subset of tests on released images to verify they are working.

## Running smoke tests on a Docker Image

To run automated smoke test on an image you can use the following command

```shell
./release/smoke-tests/run-smoke-tests.sh -v <image_tag> -i <image_name>
```

To run smoke tests on the latest published Docker image you would run the following command:

```shell
./release/smoke-tests/run-smoke-tests.sh -v latest -r opensearchproject/data-prepper
```

It is also possible to run smoke tests on a locally built image. Here is an example of targeting a local image `customImageName:myTag`. The image name (-i) is optional, the default value is `opensearch-data-prepper`.
```shell
./release/smoke-tests/run-smoke-tests.sh -v myTag -i customImageName
```

If all smoke tests complete successfully the last message printed will be "All smoke tests passed". Failing tests will result in a non-zero exit code.

## Running smoke tests on tarball files

The `run-tarball-files-smoke-tests.sh` script will smoke test a given tar archive against Docker image. Internally it uses the `run-smoke-tests.sh` script.

To run automated smoke test on the default archive file you can use the following command:

```shell
./release/smoke-tests/run-tarball-files-smoke-tests.sh
```

You can also customize what it tests against. The `-i` parameter specifies a base Docker image. The `-t` parameter determines which tar archive file to use.
The values for `-t` are `opensearch-data-prepper` or `opensearch-data-prepper-jdk`.

```shell
./release/smoke-tests/run-tarball-files-smoke-tests.sh -i openjdk:11 -t opensearch-data-prepper
./release/smoke-tests/run-tarball-files-smoke-tests.sh -i openjdk:17 -t opensearch-data-prepper
./release/smoke-tests/run-tarball-files-smoke-tests.sh -i ubuntu:latest -t opensearch-data-prepper-jdk
```

