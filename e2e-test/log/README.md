# Log Data Ingestion End-to-end Tests

This module includes e2e tests for log data ingestion supported by data-prepper.

## Basic Grok Ingestion Pipeline End-to-end test

Run from current directory
```
./gradlew :basicLogEndToEndTest
```
or from project root directory
```
./gradlew :e2e-test:log:basicLogEndToEndTest
```

## Parallel Grok and string substitute End-to-End test

Run from current directory
```
./gradlew :parallelGrokStringSubstituteTest
```
or from project root directory
```
./gradlew :e2e-test:log:parallelGrokStringSubstituteTest
```
