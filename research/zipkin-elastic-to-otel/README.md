# Zipkin trace data from ODFE to Otel Trace Source

This is a demo on reading zipkin trace analytics data from local opendistro-elasticsearch cluster to Otel Trace source.

## Prerequisite

* ODFE docker: listening on https://localhost:9200 with `admin` as user and password.
* Otel Trace Source: listening on http://127.0.0.1:21890.

## Run

```
./gradlew :research:zipkin-elastic-to-otel:run --args YOUR_INDEX_PATTERN
```

* Test mode: with environment variable test set to `true`. A test Otel trace source will be launched listening on http://127.0.0.1:21890.

```
./gradlew :research:zipkin-elastic-to-otel:run -Dtest=true --args YOUR_INDEX_PATTERN
```