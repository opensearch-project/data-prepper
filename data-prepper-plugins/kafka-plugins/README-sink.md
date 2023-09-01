# Kafka Sink

This is the Data Prepper Kafka sink plugin that reads records from buffer and publish it to Kafka topic. It uses the producer API provided by Kafka to write messages to the kafka broker.


## Usages

The Kafka sink should be configured as part of Data Prepper pipeline yaml file.

## Configuration Options

```
log-pipeline :
  source :
    random:
  sink :
    - kafka:
        bootstrap_servers:
          - "pkc-l7pr2.ap-south-1.aws.confluent.cloud:9092"
        thread_wait_time: 1000
        dlq:
          s3:
            bucket: "mydlqtestbucket"
            key_path_prefix: "dlq-files/"
            sts_role_arn: "arn:aws:iam::XXXXXXX:role/dataprepper"
            region: "ap-south-1"
        serde_format: json
        record_key: "rec1"
        topic:
           name: rajesh_test_json
	   is_create: true
           number_of_partitions: 1
           replication_factor: 3
        schema:
          registry_url: https://psrc-znpo0.ap-southeast-2.aws.confluent.cloud
          version: 1
          is_create: true
          version: 1
          schema_file_location: /home/labuser/project/avroschema.json
          basic_auth_credentials_source: USER_INFO
          schema_registry_api_key: XXXXXXX
          schema_registry_api_secret: XXXXXXXXXXXXXXXX+moAdK8xXM5/sp7eMaoiQ/sj5AKtbVFEjlu
	  inline_schema:  {json schema string}
          s3_file_config:
            bucket_name: schemaconfuration
            file_key: json_schema.txt
            region: "ap-south-1"

        producer_properties:
          buffer_memory: 102400000
          compression_type: gzip
          retries: 3
          batch_size: 16384
          client_dns_lookup: use_all_dns_ips
          connections_max_idle_ms: 540000
          delivery_timeout_ms: 120000
          linger_ms: 0
          max_block_ms: 60000
          max_request_size: 1048576
          partitioner_class: org.apache.kafka.clients.producer.internals.DefaultPartitioner
          partitioner_ignore_keys: false
          receive_buffer_bytes: 32768
          request_timeout_ms: 60000
          send_buffer_bytes: 131072
          socket_connection_setup_timeout_max_ms: 120000
          socket_connection_setup_timeout_ms: 10000
          acks: all
          enable_idempotence: true
          interceptor_classes:
            - ""
          max_in_flight_requests_per_connection: 5
          metadata_max_age_ms: 300000
          metadata_max_idle_ms: 300000
          metric_reporters:
            - ""
          metrics_num_samples: 2
          metrics_recording_level: INFO
          metrics_sample_window_ms: 30000
          partitioner_adaptive_partitioning_enable: true
          partitioner_availability_timeout_ms: 5000
          reconnect_backoff_max_ms: 1000
          reconnect_backoff_ms: 100
          retry_backoff_ms: 100
        authentication:
          sasl:
              oauth:
                  oauth_client_id: XXXXXX
                  oauth_client_secret: XXXXXXXXXXXXXXXXXXX
                  oauth_login_server: https://dev-XXXXXXX.okta.com
                  oauth_login_endpoint: /oauth2/default/v1/token
                  oauth_login_grant_type: refresh_token
                  oauth_login_scope: kafka
                  oauth_introspect_server: https://dev-XXXXXXX.okta.com
                  oauth_introspect_endpoint: /oauth2/default/v1/introspect
                  oauth_token_endpoint_url: https://dev-XXXXXXXXXX.okta.com/oauth2/default/v1/token
                  oauth_sasl_mechanism: OAUTHBEARER
                  oauth_security_protocol: SASL_SSL
                  oauth_sasl_login_callback_handler_class: org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler
                  oauth_jwks_endpoint_url: https://dev-XXXXXXXXXX.okta.com/oauth2/default/v1/keys
                  extension_logicalCluster: lkc-0jd9kq
                  extension_identityPoolId: pool-RXzn
```

## Configuration

- `bootstrap_servers` (Required) : It is a host/port to use for establishing the initial connection to the Kafka cluster. Multiple brokers can be configured.

- `thread_waiting_time` (Optional) : It is the time for thread to wait until other thread completes the task and signal it.

- `serde_format` (Optional) : It must provide serde_format(Serializer/Deserializer) for the data types of record keys and record values.
                              Possible values can be plaintext or json.

### <a name="topic_configuration">Topic Configuration</a>
- `name` (Required) : The topic in which kafka source plugin associated with to write the messages.

- `is_create` (Optional) : If this flag is enabled then topic will be created based on the given configuration.

- `number_of_partitions` (Optional) : This represents number of partitions to be configured againts the topic.

- `replication_factor` (Optional) : This replication factor tells in how many brokers to configure this topic. 

 
     

### <a name="DLQ_configuration">Dead Letter Queue (DLQ) Configuration</a>

- `bucket` (Optional) : 

- `key_path_prefix` (Optional) :

- `sts_role_arn` (Optional) : 

- `region` (Optional) : 

### <a name="schema_configuration">Schema Configuration</a>

- `registry_url` (Optional) : This can be the local schema registry URL or any other remote or cloud schema registry URL.

- `version` (Optional) : This schema version ensuring that different versions of the schema can be used simultaneously without causing compatibility issues.

- `schema_registry_api_key` (Optional) : Schema Registry API key is the username required to access the Schema Registry.

- `schema_registry_api_secret` (Optional) : Schema Registry API secret is the password required to access the Schema Registry.

- `basic_auth_credentials_source` (Optional) : It is the security configuration to authenticate for a schema registry.USER_INFO is the default value.

- `is_create` (Optional) : This flag will be enabled when schema has to created on the fly, By default it is false.

From below configurations one of them is mandatory when is_create flag is enabled.

- `schema_file_location` (Optional): This accepts a valid schema definition file path.

- `s3_file_config` (Optional): This schema configuration from s3 bucket.

- `inline_schema` (Optional): This  accepts an inline json string for schema defintion.




### <a name="schema_configuration">Producer Configuration</a>
- `max.request.size` (Optional) : The maximum size of a request in bytes.

- `retry.backoff.ms` (Optional) : The amount of time to wait before attempting to retry a failed request to a given topic partition.

- `compression.type` (Optional) : This configuration accepts the standard compression codecs 'gzip' and 'snappy'.

- `delivery.timeout.ms` (Optional) : This limits the total time that a record will be delayed prior to sending, the time to await acknowledgement from the broker, and the time allowed for retriable send failures.

- `max.block.ms` (Optional) : The configuration controls how long the KafkaProducer's send(), partitionsFor(), initTransactions(), sendOffsetsToTransaction(), commitTransaction() and abortTransaction() methods will block.

- `max.request.size` (Optional) : The maximum size of a request in bytes.

- `partitioner.class` (Optional) : A class to use to determine which partition to be send to when produce the records.

- `partitioner.ignore.keys` (Optional) : When set to 'true' the producer won't use record keys to choose a partition. If 'false', producer would choose a partition based on a hash of the key when a key is present.

- `receive.buffer.bytes` (Optional) : The size of the TCP receive buffer (SO_RCVBUF) to use when reading data. If the value is -1, the OS default will be used.

- `request.timeout.ms` (Optional) : The configuration controls the maximum amount of time the client will wait for the response of a request.

- `send.buffer.bytes` (Optional) : The size of the TCP send buffer (SO_SNDBUF) to use when sending data. If the value is -1, the OS default will be use.

- `acks` (Optional) : The number of acknowledgments the producer requires the leader to have received before considering a request complete.
  The following settings are allowed:acks=0, acks=1 and acks=all

- `enable.idempotence` (Optional) : When set to 'true', the producer will ensure that exactly one copy of each message is written in the stream. If 'false', producer retries due to broker failures, etc.

- `interceptor.classes` (Optional) : A list of classes to use as interceptors.By default, there are no interceptors.

- `max.in.flight.requests.per.connection` (Optional) : The maximum number of unacknowledged requests the client will send on a single connection before blocking.

- `metadata.max.age.ms` (Optional) : The period of time in milliseconds after which we force a refresh of metadata even if we haven't seen any partition leadership changes to proactively discover any new brokers or partitions.

- `metadata.max.idle.ms` (Optional) : Controls how long the producer will cache metadata for a topic that's idle.

- `metric.reporters` (Optional) : A list of classes to use as metrics reporters. Implementing the org.apache.kafka.common.metrics.MetricsReporter interface allows plugging in classes that will be notified of new metric creation.

- `metrics.num.samples` (Optional) : The number of samples maintained to compute metrics.

- `metrics.recording.level` (Optional) : The highest recording level for metrics.

- `metrics.sample.window.ms` (Optional) : The window of time a metrics sample is computed over.

- `partitioner.adaptive.partitioning.enable` (Optional) : When set to 'true', the producer will try to adapt to broker performance and produce more messages to partitions hosted on faster brokers. If 'false', producer will try to distribute messages uniformly.

- `partitioner.availability.timeout.ms` (Optional) : If a broker cannot process produce requests from a partition for partitioner.availability.timeout.ms time, the partitioner treats that partition as not available. If the value is 0, this logic is disabled.

- `reconnect.backoff.max.ms` (Optional) : The maximum amount of time in milliseconds to wait when reconnecting to a broker that has repeatedly failed to connect.

- `reconnect.backoff.ms` (Optional) : The base amount of time to wait before attempting to reconnect to a given host.

- `socket.connection.setup.timeout.max.ms` (Optional) : The maximum amount of time the client will wait for the socket connection to be established.

- `socket.connection.setup.timeout.ms` (Optional) : The amount of time the client will wait for the socket connection to be established. If the connection is not built before the timeout elapses, clients will close the socket channel.

- `linger.ms` (Optional) : The producer groups together any records that arrive in between request transmissions into a single batched request.

- `connections_max_idle_ms` (Optional) : Idle connections timeout: the server socket processor threads close the connections that idle more than this.

- `buffer_memory` (Optional) : The total bytes of memory the producer can use to buffer records waiting to be sent to the server.

- `batch_size` (Optional) : The producer will attempt to batch records together into fewer requests whenever multiple records are being sent to the same partition.

- `client_dns_lookup` (Optional) : Controls how the client uses DNS lookups. If set to use_all_dns_ips, connect to each returned IP address in sequence until a successful connection is established.Valid Values: [use_all_dns_ips and resolve_canonical_bootstrap_servers_only].

### <a name="auth_configuration">Auth Configuration for SASL PLAINTEXT</a>

- `username` (Optional) : The username for the Plaintext authentication.

- `password` (Optional) : The password for the Plaintext authentication.

### <a name="auth_configuration">Auth Configuration for SASL OAUTH</a>

- `oauth_client_id`: It is the client id is the public identifier of your authorization server.

- `oauth_client_secret` : It is a secret known only to the application and the authorization server.

- `oauth_login_server` : The URL of the OAuth server.(Eg: https://dev.okta.com)

- `oauth_login_endpoint`: The End point URL of the OAuth server.(Eg: /oauth2/default/v1/token)

- `oauth_login_grant_type` (Optional) : This grant type refers to the way an application gets an access token.

- `oauth_login_scope` (Optional) : This scope limit an application's access to a user's account.

- `oauth_introspect_server` (Optional) : The URL of the introspect server. Most of the cases it should be similar to the oauth_login_server URL (Eg:https://dev.okta.com)

- `oauth_introspect_endpoint` (Optional) : The end point of the introspect server URL.(Eg: /oauth2/default/v1/introspect)

- `oauth_sasl_mechanism` (Optional) : It describes the authentication mechanism.

- `oauth_security_protocol` (Optional) : It is the SASL security protocol like PLAINTEXT or SSL.

- `oauth_sasl_login_callback_handler_class` (Optional) : It is the user defined or built in Kafka class to handle login and its callbeck.

- `oauth_jwks_endpoint_url` (Optional) : The absolute URL for the oauth token refresh.

- `extension_logicalCluster` (Optional) : This is the Cluster ID.

- `extension_identityPoolId` (Optional) : This is the Pool ID which is created in the Schema Registry.

### <a name="auth_configuration">SSL with plaintext</a>
   - `username` (Optional) : The username for the Plaintext authentication.

  - `password` (Optional) : The password for the Plaintext authentication.

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

- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
