# Core Peer Forwarder End-to-end Tests

This module includes e2e tests for core peer forwarder supported by data-prepper.

## Basic Log Ingestion Pipeline End-to-end test using Local Node Peer Forwarding

Run from current directory
```
./gradlew :localAggregateEndToEndTest
```
or from project root directory
```
./gradlew :e2e-test:peerforwarder:localAggregateEndToEndTest
```

## Basic Log Ingestion Pipeline End-to-end test using Static list Peer Forwarding with SSL and mTLS

Run from current directory
```
./gradlew :staticRemoteAggregateEndToEndTest
```
or from project root directory
```
./gradlew :e2e-test:peerforwarder:staticAggregateEndToEndTest
```

## Log Ingestion Pipeline metrics End-to-end test using Static list Node Peer Forwarding

Run from current directory
```
./gradlew :staticLogMetricsEndToEndTest
```
or from project root directory
```
./gradlew :e2e-test:peerforwarder:staticLogMetricsEndToEndTest
```
