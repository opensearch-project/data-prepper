# Trace Data Ingestion End-to-end Tests

This module includes e2e tests for trace data ingestion supported by data-prepper.

## Raw Span Ingestion Pipeline End-to-end test

### OTLP record type

Run from current directory
```
./gradlew :rawSpanOTLPEndToEndTest
```
or from project root directory
```
./gradlew :e2e-test:trace:rawSpanOTLPEndToEndTest
```

### Event record type compatibility with OTLP record type

Run from current directory
```
./gradlew :rawSpanOTLPAndEventEndToEndTest
```
or from project root directory
```
./gradlew :e2e-test:trace:rawSpanOTLPAndEventEndToEndTest
```

## Raw Span Ingestion Pipelines Latest Release Compatibility End-to-end test

### OTLP record type compatibility with latest release

Run from current directory
```
./gradlew :rawSpanOTLPLatestReleaseCompatibilityEndToEndTest
```
or from project root directory
```
./gradlew :e2e-test:trace:rawSpanOTLPLatestReleaseCompatibilityEndToEndTest
```

### Event record type compatibility with latest release

Run from current directory
```
./gradlew :rawSpanEventLatestReleaseCompatibilityEndToEndTest
```
or from project root directory
```
./gradlew :e2e-test:trace:rawSpanEventLatestReleaseCompatibilityEndToEndTest
```

## Service Map Ingestion Pipelines End-to-end test

### OTLP record type

Run from current directory
```
./gradlew :serviceMapOTLPEndToEndTest
```
or from project root directory
```
./gradlew :e2e-test:trace:serviceMapOTLPEndToEndTest
```

### Event record type compatibility with OTLP record type

Run from current directory
```
./gradlew :serviceMapOTLPAndEventEndToEndTest
```
or from project root directory
```
./gradlew :e2e-test:trace:serviceMapOTLPAndEventEndToEndTest
```