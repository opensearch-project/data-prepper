# Trace Data Ingestion End-to-end Tests

This module includes e2e tests for trace data ingestion supported by data-prepper.

## Raw Span Ingestion Pipeline End-to-end test

Run from current directory
```
./gradlew :rawSpanEndToEndTest
```
or from project root directory
```
./gradlew :e2e-test:trace:rawSpanEndToEndTest
```

## Raw Span Ingestion Pipelines Latest Release Compatibility End-to-end test

Run from current directory
```
./gradlew :rawSpanLatestReleaseCompatibilityEndToEndTest
```
or from project root directory
```
./gradlew :e2e-test:trace:rawSpanLatestReleaseCompatibilityEndToEndTest
```

## Service Map Ingestion Pipelines End-to-end test

```
./gradlew :serviceMapEndToEndTest
```
or from project root directory
```
./gradlew :e2e-test:trace:serviceMapEndToEndTest
```
