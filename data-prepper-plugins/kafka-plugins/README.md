# Kafka source

This source allows Data Prepper to use Kafka as source. This source reads records from one or more Kafka topics. It uses the consumer API provided by Kafka to read messages from the kafka broker to create DataPrepper events for further processing by the Data Prepper pipeline.

## Basic Usage and Configuration

For usage and configuration, please refer to the documentation [here] (https://opensearch.org/docs/2.9/data-prepper/pipelines/configuration/sources/sources/kafka-source).


## Integration Tests

Before running the integration tests, make sure Kafka server is started
1. Start Zookeeper
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
2. Start Kafka Server with the following configuration
Configuration in config/server.properties
```
isteners=SASL_SSL://localhost:9093,PLAINTEXT://localhost:9092,SSL://localhost:9094,SASL_PLAINTEXT://localhost:9095
security.inter.broker.protocol=SASL_SSL
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.enabled.mechanisms=PLAIN
ssl.truststore.location=<location of truststore>
ssl.truststore.password=<password of truststore>
ssl.keystore.location=<location of keystore>
ssl.keystore.password=<password of keystore>
```
The truststore must have "localhost" certificates in them.

3. Create a file with name `kafka_server_jaas.conf` and with the following contents:
```
KafkaClient {
    org.apache.kafka.common.security.plain.PlainLoginModule required
    username="admin"
    password="admin"
    user_admin="admin";
};
```

4. Export `KAFKA_OPTS` environment variable
```
export KAFKA_OPTS=-Djava.security.auth.login.config=kafka_server_jaas.conf
```

5. start kafka server
```
bin/kafka-server-start.sh config/server.properties
```

6. Command to run multi auth type integration tests

```
./gradlew    data-prepper-plugins:kafka-plugins:integrationTest -Dtests.kafka.bootstrap_servers=localhost:9092 -Dtests.kafka.saslssl_bootstrap_servers=localhost:9093 -Dtests.kafka.ssl_bootstrap_servers=localhost:9094 -Dtests.kafka.saslplain_bootstrap_servers=localhost:9095 -Dtests.kafka.username=admin -Dtests.kafka.password=admin --tests "*KafkaSourceMultipleAuthTypeIT*"
```

7. Command to run msk glue integration tests

```
./gradlew     data-prepper-plugins:kafka-plugins:integrationTest -Dtests.kafka.bootstrap_servers=<msk-bootstrap-servers> -Dtests.kafka.glue_registry_name=<glue-registry-name> -Dtests.kafka.glue_avro_schema_name=<glue-registry-avro-schema-name> -Dtests.kafka.glue_json_schema_name=<glue-registry-json-schema-name> -Dtests.msk.region=<msk-region> -Dtests.msk.arn=<msk-arn>  --tests "*TestAvroRecordConsumer*" 

```

## Developer Guide

This plugin is compatible with Java 11. See

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
