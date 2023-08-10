# Kafka source

This source allows Data Prepper to use Kafka as source. This source reads records from one or more Kafka topics. It uses the consumer API provided by Kafka to read messages from the kafka broker to create DataPrepper events for further processing by the Data Prepper pipeline.

## Basic Usage
The following pipeline configuration will read plain string messages from two configured Kafka topics `Topic1` and `Topic2`

```
kafka-pipeline:
  source:
    kafka:
      bootstrap_servers:
        - 127.0.0.1:9093
      topics:
        - name: Topic1
          group_id: groupID1
        - name: Topic2
          group_id: groupID1
```


## Configuration Options

* `bootstrap_servers` (Required when not using MSK) : It is a host/port to use for establishing the initial connection to the Kafka cluster. Multiple brokers can be configured. When using MSK as the Kafka cluster, bootstrap server information is obtained from the MSK using MSK ARN provided in the config.

* `topics` (Required) : List of topics to read the messages from. The maximum number of topics is 10. See [Topic Configuration](#topic_configuration) for details.

* `schema` (Optional) : Schema Registry Configuration. See [Schema Registry Configuration](#schema_configuration) for details.

* `authentication` (Optional) : Authentication Configuration. See [Authentication Configuration](#authentication_configuration) for details.

* `encryption` (Optional) : Encryption configuration. See [Encryption Configuration](#encryption_configuration) for details.

* `aws` (Optional) : AWS configurations. See [AWS Configuration](#aws_configuration) for details.

* `acknowledgments` (Optional) : Enables End-to-end acknowledgments. If set to `true`, sqs message is deleted only after all events from the sqs message are successfully acknowledged by all sinks. Default value `false`.

* `acknowledgments_timeout` (Optional) : Maximum time to wait for the acknowledgements to be received. Default value is `30s`.

* `client_dns_lookup`: Sets Kafka's client.dns.lookup option. This is needed when DNS aliases are used. Default value is `default`.

### <a name="topic_configuration">Topic Configuration</a>

* `name` (Required) : This denotes the name of the topic, and it is a mandatory one. Multiple list can be configured and the maximum number of topic should be 10.

* `group_id` (Required) : Sets Kafka's group.id option.

* `workers` (Optional) : Number of multithreaded consumers associated with each topic. Defaults value `2`. Maximum value is 200.

* `serde_format` (Optional): Indicates the serialization and deserialization format of the messages in the topic. Default value is `plaintext`.

* `auto_commit` (Optional) : If false, the consumer's offset will not be periodically committed in the background. Default value `false`.

* `commit_interval` (Optional) : The frequency in seconds that the consumer offsets are committed to Kafka. Used to set Kafka's auto.commit.interval.ms option if `auto_commit` is enabled. Used as commit interval when auto commit is disabled. Default value `5s`.

* `session_timeout` (Optional) : The timeout used to detect client failures when using Kafka's group management. It is used for the rebalance. Default value `45s`

* `auto_offset_reset` (Optional) : Sets Kafka's `auto.offset.reset` option. Default value `latest`.

* `thread_waiting_time` (Optional) : It is the time for thread to wait until other thread completes the task and signal it. Kafka consumer poll timeout value is set to half of `thread_waiting_time`. Default value `5s`

* `max_partition_fetch_bytes` (Optional) : Sets Kafka's max.partition.fetch.bytes option. Default value `1048576` (1MB).

* `heart_beat_interval` (Optional) : The expected time between heartbeats to the consumer coordinator when using Kafka's group management facilities. Used to set Kafka's heartbeat.interval.ms option. Defaults to `1s`.

* `fetch_max_bytes` (Optional) : The maximum record batch size accepted by the broker. Sets Kafka's fetch.max.bytes option.  Default value `52428800`.

* `fetch_max_wait` (Optional) : The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy the requirement. Sets Kafka's fetch.max.wait.ms option. Default value `500`.

* `fetch_min_bytes` (Optional) : The minimum amount of data the server should return for a fetch request. Sets Kafka's fetch.min.bytes option. Default value `1`.

* `retry_backoff` (Optional) : The amount of time to wait before attempting to retry a failed request to a given topic partition. Sets Kafka's retry.backoff.ms option. Default value `10s`.

* `reconnect_backoff` (Optional) : Sets Kafka's reconnect.backoff.ms option. Default value `10s`.

* `max_poll_interval` (Optional) : The maximum delay between invocations of poll() when using consumer group management. Sets Kafka's max.poll.interval.ms option. Defaults to `300s`.

* `consumer_max_poll_records` (Optional) : The maximum number of records returned in a single call to poll(). Sets Kafka's max.poll.records option. Defaults to `500`.

* `key_mode` (Optional) : This indicates how the key field of the kafka message be handled. Default value for this is `include_as_field` which means the key is included in the event as `kafka_key`, if `discard` mode is used, the key is entirely discarded. if `include_as_metadata` is used, the key is put in the event metadata.

### <a name="schema_configuration">Schema Configuration</a>

* `type` (Required) : Valid types are `glue` and `confluent`. `glue` should be specified when using AWS Glue Registry and `confluent` should be specified when using Confluent schema registry. When using `glue` registry, aws config options under the `aws` section are used. 

The following config options are valid only for the Confluent Registry

* `registry_url` (Required) : Deserialize a record value from a bytearray into a String. Defaults to `org.apache.kafka.common.serialization.StringDeserializer`.

* `version` (Required) : Deserialize a record key from a bytearray into a String. Defaults to `org.apache.kafka.common.serialization.StringDeserializer`.

* `schema_registry_api_key` (Required) : Schema Registry API key. Used in the secure communication with the schema registry.

* `schema_registry_api_secret` (Required) : Schema Registry API secret. Used in the secure communication with the schema registry.

### <a name="authentication_configuration">Authentication Configuration</a>

* `sasl` (Required) : SASL authentication configuration. See [SASL Configuration](#sasl_configuration) for details.

### <a name="sasl_configuration">SASL Configuration</a>

One of the following options is required.

* `plaintext` (Optional) : Plaintext configuration. See [Plaintext Configuration](#plaintext_configuration) for details.

* `aws_msk_iam` (Optional) : AWS MSK IAM configuration. This can take either `role` or `default` values. When `role` option is used, the `sts_role_arn` used in the `aws` config is used to assume the role. Default value is `default`.

### <a name="plaintext_configuration">Plaintext Configuration</a>

- `username` (Required) : A String of username to be used for Kafka cluster authentication.

- `password` (Required) : A String of password to be used for Kafka cluster authentication.

### <a name="encryption_configuration">Encryption Configuration</a>

* `type` (Optional) : Encryption Type. Default value is `ssl`. Use `none` to disable encryption.

* `insecure` (Optional) : A boolean flag to turn off SSL certificate verification. If set to true, CA certificate verification will be turned off and insecure requests will be sent. Default to `false`.

### <a name="aws_configuration">AWS Configuration</a>

* `region` (Optional) : The AWS region to use for credentials. Defaults to [standard SDK behavior to determine the region](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/region-selection.html).
* `sts_role_arn` (Optional) : The AWS STS role to assume for requests to SQS. Defaults to null, which will use the [standard SDK behavior for credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html). 
* `msk` (Optional) : MSK configurations. See [MSK Configuration](#msk_configuration) for details.

### <a name="msk_configuration">MSK Configuration</a>

* `arn` (Required) : The MSK ARN to use.

* `broker_connection_type` (Optional) : type of connection to use with the MSK broker. Allowed values are `public`, `single_vpc` and `multi_vpc`. Default value is `single_vpc`.


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

Command to start kafka server
```
bin/kafka-server-start.sh config/server.properties
```

3. Command to run multi auth type integration tests

```
./gradlew    data-prepper-plugins:kafka-plugins:integrationTest -Dtests.kafka.bootstrap_servers=<bootstrap-servers> -Dtests.kafka.trust_store_location=</path/to/client.truststore.jks> -Dtests.kafka.trust_store_password=<password> -Dtests.kafka.saslssl_bootstrap_servers=<sasl-bootstrap-server> -Dtests.kafka.ssl_bootstrap_servers=<ssl-bootstrap-servers> -Dtests.kafka.saslplain_bootstrap_servers=<plain-bootstrap-servers> -Dtests.kafka.username=<username> -Dtests.kafka.password=<password> --tests "*KafkaSourceMultipleAuthTypeIT*"
```

4. Command to run msk glue integration tests

```
./gradlew     data-prepper-plugins:kafka-plugins:integrationTest -Dtests.kafka.bootstrap_servers=<msk-bootstrap-servers> -Dtests.kafka.glue_registry_name=<glue-registry-name> -Dtests.kafka.glue_avro_schema_name=<glue-registry-avro-schema-name> -Dtests.kafka.glue_json_schema_name=<glue-registry-json-schema-name> -Dtests.msk.region=<msk-region> -Dtests.msk.arn=<msk-arn>  --tests "*TestAvroRecordConsumer*" 

```

## Developer Guide

This plugin is compatible with Java 11. See

* [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
* [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)

