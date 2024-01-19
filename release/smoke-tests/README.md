# Smoke Tests

This directory contains smoke tests for Data Prepper. Data Prepper smoke tests perform very basic validation of artifacts to ensure that they were
built correctly. The unit, integration, and end-to-end tests cover for correct functionality.

## Running smoke tests on a Docker Image

To run automated smoke test on an image you can use the following command

```shell
./release/smoke-tests/run-smoke-tests.sh -v <image_tag> -r <image_repository>
```

To run smoke tests on the latest published docker image you would run the following command:

```shell
./release/smoke-tests/run-smoke-tests.sh -v latest -r opensearchproject
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

## Troubleshooting smoke tests

If the script is stuck repeating the message "Waiting for Data Prepper to start" try the following steps

### 1. Confirm all containers are running with `docker ps`. Your output should be similar to the following:
```
CONTAINER ID   IMAGE                                   COMMAND                  CREATED         STATUS                          PORTS                                                                     NAMES
f3d476e2676d   smoke-tests_otel-span-exporter          "/bin/sh -c 'python3…"   5 minutes ago   Restarting (0) 54 seconds ago                                                                             smoke-tests_otel-span-exporter_1
360db2978df1   otel/opentelemetry-collector:0.40.0     "/otelcol --config=/…"   5 minutes ago   Up 5 minutes                    0.0.0.0:4317->4317/tcp, :::4317->4317/tcp, 55678-55679/tcp                smoke-tests_otel-collector_1
b012352c7593   opensearchproject/data-prepper:latest   "/bin/sh -c 'java $J…"   5 minutes ago   Up 5 minutes                    0.0.0.0:2021->2021/tcp, :::2021->2021/tcp                                 smoke-tests_data-prepper_1
539741e51931   opensearchproject/opensearch:1.0.0      "./opensearch-docker…"   5 minutes ago   Up 5 minutes                    9300/tcp, 9600/tcp, 0.0.0.0:9200->9200/tcp, :::9200->9200/tcp, 9650/tcp   node-0.example.com
3b5b1f974174   alpine                                  "/bin/sh -c 'set -x;…"   5 minutes ago   Up 5 minutes                                                                                              smoke-tests_http-log-generation_1
```
### 2. Check the container logs, these commands should be executed from ./release/smoke-test.
Tail all container logs:
```
docker-compose logs -f
```

Follow specific container logs:
```
docker-compose logs -f data-prepper
```

The Data Prepper container should show events being processed:
```
...
data-prepper_1         | 2021-12-03T19:57:30,488 [service-map-pipeline-prepper-worker-3-thread-1] INFO  org.opensearch.dataprepper.pipeline.ProcessWorker -  service-map-pipeline Worker: No records received from buffer
data-prepper_1         | 2021-12-03T19:57:31,925 [grok-pipeline-prepper-worker-7-thread-1] INFO  org.opensearch.dataprepper.pipeline.ProcessWorker -  grok-pipeline Worker: Processing 8 records from buffer
...
```
### 3. Confirm Open Search is running
```
curl -s -k -u 'admin:<admin password>' 'https://localhost:9200/_cat/indices'
```
If indicies are displayed Open Search is running.
```
yellow open security-auditlog-2021.12.03   8FGpNXmYRamkbuOsdTRORQ 1 1 13 0 75.1kb 75.1kb
yellow open test-grok-index                WRuIkpsiQXOcGirpp9hRqw 1 1 77 0 39.1kb 39.1kb
yellow open .opendistro-job-scheduler-lock cyeA9SeST5ibZQPW24CJPA 1 1  1 1   11kb   11kb
yellow open otel-v1-apm-service-map        uHdBor9FTSiJDwLAHLroVw 1 1  0 0   208b   208b
green  open .opendistro_security           IAWYPcinRz66_mamfcLsew 1 0  9 0 57.8kb 57.8kb
yellow open otel-v1-apm-span-000001        3pN9V1YNSjSMY5ggHa2TfA 1 1 12 0 28.3kb 28.3kb
```
**To confirm if Open Search is receiving log data run**
Use the following cURL command to query the index `test-grok-index` for any documents. **Note**: The results may not show all documents received.
```
curl -k -u 'admin:<admin password>' https://localhost:9200/test-grok-index/_search
```
If in your results the JSON path `.hits.total.value` has a value of 0 Open Search is not receiving log data. Confirm with the Data Prepper logs records are being processed from the buffer and no error messages are displayed.

**To confirm if Open Search is receiving trace data run**
Use the following cURL command to query the `otel-v1-apm-span-000001` index for documents containing **PythonService**. These documents will be generated by the **otel-span-exporter**, sent to **otel-collector**, then sent to **data-prepper**, then sent to **opensearch**.
```
curl -k -u 'admin:<admin password>' https://localhost:9200/otel-v1-apm-span-000001/_search?q=PythonService
```
If in your results the JSON path `.hits.total.value` has a value of 0 Open Search is not receiving trace data. Confirm OTel opentelemetry-collector logs are continueally displaying metrics and no errors are printing. Next confirm with the Data Prepper logs records are being processed from the buffer and no error messages are displayed.

### 4. Manually send data to Data Prepper
The following cURL command will send a JSON formatted HTTP log to Data Prepper:
```
curl -k \
    -H "Content-Type: application/json" \
    -d "[{\"log\": \"smoke test log\"}]" \
    "http://localhost:2021/log/ingest"
```
