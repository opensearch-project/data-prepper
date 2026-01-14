# Kafka Backward Compatibility End-to-End Test

## Overview

This test verifies that the current build of Data Prepper can successfully read and process messages written to Kafka by previous released versions.

## Test Scenario

### Phase 1: Write with Released Version
1. Start Kafka container
2. Start **released** Data Prepper from Docker Hub (e.g., `opensearchproject/data-prepper:2.10.0`)
3. Send 2 test records via HTTP endpoint
4. Records are written to Kafka buffer using the released version's format
5. Stop the released Data Prepper container

### Phase 2: Read with Current Build
1. Start **current** Data Prepper (built from source)
2. Data Prepper reads the 2 messages from Kafka
3. Messages are processed and written to OpenSearch

### Phase 3: Verification
1. Query OpenSearch to verify both records were correctly processed
2. Validate message content matches expected values

## Running the Test

### Prerequisites
- Docker running locally
- Java 11 or later
- Gradle

### Run the Test

```bash
# From the repository root
./gradlew :e2e-test:kafka-backward-compatibility:kafkaBackwardCompatibilityTest
```

### Run with Specific Version

You can test backward compatibility with a specific Data Prepper version:

```bash
./gradlew :e2e-test:kafka-backward-compatibility:kafkaBackwardCompatibilityTest \
  -PbackwardCompatVersion=2.9.0
```

## Test Components

### Docker Containers

1. **Kafka** (`kafka-backward-compat-test`)
   - Confluent Kafka 3.6.0
   - KRaft mode (no ZooKeeper)
   - Port: 9092

2. **OpenSearch** (`node-0.example.com`)
   - OpenSearch 1.3.14
   - Port: 9200

3. **Released Data Prepper** (`data-prepper-writer`)
   - Pulled from Docker Hub
   - Writes data to Kafka

4. **Current Data Prepper** (`data-prepper-reader`)
   - Built from current source
   - Reads data from Kafka

### Configuration Files

- **`writer-pipeline.yaml`**: HTTP source → Kafka buffer
- **`reader-pipeline.yaml`**: Kafka source → OpenSearch sink
- **`data-prepper-config.yaml`**: Basic Data Prepper configuration

## What This Tests

✅ **Kafka message format compatibility**
- Protobuf serialization format
- Message envelope structure
- Field naming and types

✅ **Consumer offset management**
- Offset commits and reads
- Consumer group handling

✅ **Data integrity**
- Messages written by old version can be read by new version
- No data loss or corruption
- Field values preserved correctly

## Troubleshooting

### View Docker Logs

```bash
# Kafka logs
docker logs kafka-backward-compat-test

# Released Data Prepper logs
docker logs data-prepper-writer

# Current Data Prepper logs
docker logs data-prepper-reader

# OpenSearch logs
docker logs node-0.example.com
```

### Manual Container Control

```bash
# Start Kafka manually
./gradlew :e2e-test:kafka-backward-compatibility:startKafkaDockerContainer

# Start released Data Prepper manually
./gradlew :e2e-test:kafka-backward-compatibility:startReleasedDataPrepperContainer

# Start current Data Prepper manually
./gradlew :e2e-test:kafka-backward-compatibility:startCurrentDataPrepperContainer

# Stop all
./gradlew :e2e-test:kafka-backward-compatibility:stopKafkaDockerContainer
./gradlew :e2e-test:kafka-backward-compatibility:stopReleasedDataPrepperContainer
./gradlew :e2e-test:kafka-backward-compatibility:stopCurrentDataPrepperContainer
```

### Check Kafka Topics

```bash
docker exec kafka-backward-compat-test kafka-topics --list --bootstrap-server localhost:9092
```

### Query OpenSearch Directly

```bash
curl -k -u admin:admin "https://localhost:9200/backward-compat-test-index/_search?pretty"
```

## Expected Behavior

When the test passes:
1. Released Data Prepper successfully writes 2 messages to Kafka
2. Current Data Prepper successfully reads both messages from Kafka
3. Both messages appear in OpenSearch with correct content
4. Test completes with ✅ SUCCESS message

## CI/CD Integration

This test should be run:
- ✅ Before releasing new versions
- ✅ When making changes to Kafka buffer serialization
- ✅ When upgrading Kafka client libraries
- ✅ As part of the release candidate verification process

## Notes

- The test uses **unencrypted** Kafka messages for simplicity
- If you need to test encrypted backward compatibility, modify the pipeline configs to enable encryption
- The released version can be configured via `-PbackwardCompatVersion` property
- Default released version is `2.10.0` (configured in `build.gradle`)
