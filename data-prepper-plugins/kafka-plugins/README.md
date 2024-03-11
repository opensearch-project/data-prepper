# Kafka source

This source allows Data Prepper to use Kafka as source. This source reads records from one or more Kafka topics. It uses the consumer API provided by Kafka to read messages from the kafka broker to create DataPrepper events for further processing by the Data Prepper pipeline.

## Basic Usage and Configuration

For usage and configuration, please refer to the documentation [here] (https://opensearch.org/docs/2.9/data-prepper/pipelines/configuration/sources/sources/kafka-source).


## Developer guide

### Integration tests

#### Run Kafka

You can run Kafka 3.5.1 using the KRaft protocol.

```
docker compose --project-directory data-prepper-plugins/kafka-plugins/src/integrationTest/resources/kafka/kraft up -d
```

To run Kafka 2.8.1, you must run using Zookeeper.

```
docker compose --project-directory data-prepper-plugins/kafka-plugins/src/integrationTest/resources/kafka/zookeeper up -d
```

#### Run tests

Not all integration tests currently work with Docker. But, you can run the following.

```
./gradlew data-prepper-plugins:kafka-plugins:integrationTest -Dtests.kafka.bootstrap_servers=localhost:9092 -Dtests.kafka.authconfig.username=admin -Dtests.kafka.authconfig.password=admin -Dtests.kafka.kms_key=alias/DataPrepperTesting --tests KafkaSourceJsonTypeIT --tests '*kafka.buffer*'
```

If you do not have a KMS key, you can skip the KMS tests.

```
./gradlew data-prepper-plugins:kafka-plugins:integrationTest -Dtests.kafka.bootstrap_servers=localhost:9092 -Dtests.kafka.authconfig.username=admin -Dtests.kafka.authconfig.password=admin --tests KafkaSourceJsonTypeIT --tests KafkaBufferIT --tests KafkaBufferOTelIT
```


See the Old integration tests section to run other tests. However, these are more involved.

### Old integration tests

**NOTE** We are trying to move away from these.

Before running the integration tests, make sure Kafka server is started
1. Start Zookeeper
```
bin/zookeeper-server-start.sh config/zookeeper.properties
```
2. Start Kafka Server with the following configuration
Configuration in config/server.properties
```
listeners=SASL_SSL://localhost:9093,PLAINTEXT://localhost:9092,SSL://localhost:9094,SASL_PLAINTEXT://localhost:9095
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

8. Command to run confluent integration tests (without schema registry)

```
./gradlew    data-prepper-plugins:kafka-plugins:integrationTest -Dtests.kafka.bootstrap_servers=<confluent-bootstrap-servers> -Dtests.kafka.topic_name=<topic-name> -Dtests.kafka.username=<confluent-cloud-api-key> -Dtests.kafka.password=<confluent-cloud-api-secret> --tests "*KafkaProducerConsumerIT*"
```

9. Command to run confluent integration tests with schema registry
Before issuing the command, make sure the `json-topic-name` and `avro-topic-name` are already created on the Confluent cloud and schemas are correctly set

```
./gradlew      data-prepper-plugins:kafka-plugins:integrationTest -Dtests.kafka.bootstrap_servers=<confluent-bootstrap-servers> -Dtests.kafka.schema_registry_url=<confluent-schema-reg-url> -Dtests.kafka.schema_registry_userinfo="<confluent-schema-reg-api-key>:<confluent-schema-reg-secret>" -Dtests.kafka.json_topic_name=<json-topic-name> -Dtests.kafka.avro_topic_name=<avro-topic-name> -Dtests.kafka.username=<confluent-cloud-api-key> -Dtests.kafka.password=<confluent-cloud-api-secret> --tests "*ConfluentKafkaProducerConsumerWithSchemaRegistryIT*"
```

Schema for `json-topic-name-value` should be
```
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "additionalProperties": false,
  "properties": {
    "id": {
      "oneOf": [
        {
          "title": "Not included",
          "type": "null"
        },
        {
          "type": "integer"
        }
      ]
    },
    "name": {
      "oneOf": [
        {
          "title": "Not included",
          "type": "null"
        },
        {
          "type": "string"
        }
      ]
    },
    "value": {
      "oneOf": [
        {
          "title": "Not included",
          "type": "null"
        },
        {
          "type": "number"
        }
      ]
    }
  },
  "title": "User Record",
  "type": "object"
}
```

Schema for `avro-topic-name-value` should be
```
{
  "fields": [
    {
      "name": "message",
      "type": "string"
    },
    {
      "name": "ident",
      "type": "int"
    },
    {
      "name": "score",
      "type": "double"
    }
  ],
  "name": "sampleAvroRecord",
  "type": "record"
}
```
