# Kafka source

This is the Data Prepper Kafka source plugin that reads records from Kafka topic. It uses the consumer API provided by Kafka to read messages from the broker.

The Kafka source plugin supports OpenSearch 2.0.0 and greater.

## Usages

The Kafka source should be configured as part of Data Prepper pipeline yaml file.

## Configuration Options

```
log-pipeline:
  source:
    kafka:
      bootstrap_servers:
        - 127.0.0.1:9092,localhost:9093, 127.0.0.1:9094
      topics:
        - topic:
            name: my-topic-2
            auth_type: SASL_OAUTH #optional
            consumer:
              group_name: kafka-consumer-group-2
              group_id: DPKafkaProj-2
              workers: 10 #optional and default is 10
              autocommit: false  #optional and dafault is false
              autocommit_interval: 5s  #optional and dafault is 5s
              session_timeout: 45s  #optional and dafault is 45s
              max_retry_attempts: 1000 #optional and dafault is 5
              max_retry_delay: 1s #optional and dafault is 5
              auto_offset_reset: earliest  #optional and dafault is earliest
              thread_waiting_time: 1s  #optional and dafault is 1s
              max_record_fetch_time: 4s #optional and dafault is 4s
              heart_beat_interval: 3s  #optional and dafault is 3s
              buffer_default_timeout: 5s  #optional and dafault is 5s
              fetch_max_bytes: 52428800  #optional and dafault is 52428800
              fetch_max_wait: 500  #optional and dafault is 500
              fetch_min_bytes: 1  #optional and dafault is 1
              retry_backoff: 100s  #optional and dafault is 10s
              max_poll_interval: 300000s  #optional and dafault is 300000s
              consumer_max_poll_records: 500  #optional and dafault is 500
            schema:
              registry_url: http://localhost:8081/
              key_deserializer: org.apache.kafka.common.serialization.StringDeserializer
              value_deserializer: org.apache.kafka.common.serialization.StringDeserializer
              schema_type: plaintext
              record_type: plaintext 
         - topic:
            name: my-topic-1
            auth_type: sasl_ssl
            consumer:
              group_name: kafka-consumer-group-1
              group_id: DPKafkaProj-1
              workers: 10
            schema:
              registry_url: http://localhost:8081/
              key_deserializer: org.apache.kafka.common.serialization.StringDeserializer
              value_deserializer: org.apache.kafka.common.serialization.StringDeserializer
              schema_type: plaintext
              record_type: plaintext
  sink:
    - stdout:
```

## Configuration

- `bootstrap_servers` (Required) : It is a host/port to use for establishing the initial connection to the Kafka cluster.

- `topics` (Required) : The topic in which kafka source plugin associated with to read the messages.The maximum number of topics should be 10.

- `topic name` (Required) : This denotes the name of the topic and it is a mandatory one.

- `auth_type` (Optional) : The auth_type directive selects the method that is used to authenticate the user. There is no auth type by default. The most common supported methods are SASL_PLAINTEXT,SASL_SSL,SASL_OAUTH

- `group_name`  (Required) : A consumer group name which this kafka consumer belongs to. 

- `group_id` (Required) : A consumer group id which this kafka consumer is associated with. 

- `workers` (Optional) : Number of multithreaded consumers associated with each topic. Defaults to `10` and its maximum value should be 200.

- `autocommit` (Optional) : If false the consumer's offset will not be periodically committed in the background. Defaults to `false`.

- `autocommit_interval` (Optional) : The frequency in seconds that the consumer offsets are auto-committed to Kafka. Defaults to `1s`.

- `session_timeout` (Optional) : The timeout used to detect client failures when using Kafka's group management. It is used for the rebalance.

- `max_retry_attempts` (Optional) : The maximum attempts to retry a failed write operation to the BUFFER. Defaults to `INFINITE`.

- `max_retry_delay` (Optional) : By default the Kafka source will retry for every 1 second when there is a buffer write error. Defaults to `1s`. 

- `auto_offset_reset` (Optional) : automatically reset the offset to the earliest/latest offset. Defaults to `earliest`.

- `thread_waiting_time` (Optional) : It is the time for thread to wait until other thread completes the task and signal it.

- `max_record_fetch_time` (Optional) : maximum time to fetch the record from the topic.
Defaults to `4s`.

- `heart_beat_interval` (Optional) : The expected time between heartbeats to the consumer coordinator when using Kafka's group management facilities. Defaults to `1s`.

- `buffer_default_timeout` (Optional) :  The maximum time to write data to the buffer. Defaults to `1s`.

- `fetch_max_bytes` (Optional) : The maximum record batch size accepted by the broker. 
Defaults to `52428800`.

- `fetch_max_wait` (Optional) : The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy the requirement. Defaults to `500`.

- `fetch_min_bytes` (Optional) : The minimum amount of data the server should return for a fetch request. Defaults to `1`.

- `retry_backoff` (Optional) : The amount of time to wait before attempting to retry a failed request to a given topic partition.  Defaults to `5s`.

- `max_poll_interval` (Optional) : The maximum delay between invocations of poll() when using consumer group management. Defaults to `1s`.

- `consumer_max_poll_records` (Optional) : The maximum number of records returned in a single call to poll(). Defaults to `1s`.

### <a name="schema_configuration">Schema Configuration</a>

- `key_deserializer` (Optional) : Deserialize a record value from a bytearray into a String. Defaults to `org.apache.kafka.common.serialization.StringDeserializer`.

- `value_deserializer` (Optional) : Deserialize a record key from a bytearray into a String. Defaults to `org.apache.kafka.common.serialization.StringDeserializer`.

- `schema_type` (Optional) : The type of schema format reading from the broker. This Kafka Consumer plugin supports String and json schema types. Defaults to `plaintext`.

- `record_type` (Optional) : The type of record format reading from the broker. This Kafka Consumer plugin supports String and json record types. Defaults to `plaintext`.

## Developer Guide

This plugin is compatible with Java 8. See

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md) 
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
