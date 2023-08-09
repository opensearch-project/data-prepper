# Kafka source

This source allows Data Prepper to use Kafka as source. This source reads records from one or more Kafka topics. It uses the consumer API provided by Kafka to read messages from the kafka broker.

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


## Configuration 

### Options

- `bootstrap_servers` (Required when not using MSK) : It is a host/port to use for establishing the initial connection to the Kafka cluster. Multiple brokers can be configured. When using MSK as the Kafka cluster, bootstrap server information is obtained from the MSK using MSK ARN provided in the config.

- `topics` (Required) : List of topics to read the messages from. The maximum number of topics should be 10.

- `name` (Required) : This denotes the name of the topic, and it is a mandatory one. Multiple list can be configured and the maximum number of topic should be 10.

- `workers` (Optional) : Number of multithreaded consumers associated with each topic. Defaults value `2`. Maximum value is 200.

- `auto_commit` (Optional) : If false, the consumer's offset will not be periodically committed in the background. Default value `false`.

- `commit_interval` (Optional) : The frequency in seconds that the consumer offsets are auto-committed to Kafka. Default value `5s`.

- `session_timeout` (Optional) : The timeout used to detect client failures when using Kafka's group management. It is used for the rebalance. Default value `45s`

- `auto_offset_reset` (Optional) : Sets Kafka's `auto.offset.reset` option. Default value `latest`.

- `thread_waiting_time` (Optional) : It is the time for thread to wait until other thread completes the task and signal it. Kafka consumer poll timeout value is set to half of `thread_waiting_time`. Default value `5s`

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

- `registry_url` (Optional) : Deserialize a record value from a bytearray into a String. Defaults to `org.apache.kafka.common.serialization.StringDeserializer`.

- `version` (Optional) : Deserialize a record key from a bytearray into a String. Defaults to `org.apache.kafka.common.serialization.StringDeserializer`.

### <a name="auth_configuration">Auth Configuration for SASL PLAINTEXT</a>

- `username` (Optional) : The username for the Plaintext authentication.

- `password` (Optional) : The password for the Plaintext authentication.

### <a name="auth_configuration">OAuth Configuration for SASLOAUTH</a>

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

```
log-pipeline:
  source:
    kafka:
      bootstrap_servers:
        - 127.0.0.1:9093
      topics:
        - name: my-topic-1
          workers: 10
          autocommit: false
          autocommit_interval: 5s
          session_timeout: 45s
          max_retry_delay: 1s
          auto_offset_reset: earliest
          thread_waiting_time: 1s
          max_record_fetch_time: 4s
          heart_beat_interval: 3s
          buffer_default_timeout: 5s
          fetch_max_bytes: 52428800
          fetch_max_wait: 500
          fetch_min_bytes: 1
          retry_backoff: 100s
          max_poll_interval: 300000s
          consumer_max_poll_records: 500
        - name: my-topic-2
          workers: 10
      schema:
        registry_url: http://localhost:8081/
        version: 1
      authentication:
        sasl_plaintext:
          username: admin
          password: admin-secret
        sasl_oauth:
          oauth_client_id: 0oa9wc21447Pc5vsV5d8
          oauth_client_secret: aGmOfHqIEvBJGDxXAOOcatiE9PvsPgoEePx8IPPb
          oauth_login_server: https://dev-1365.okta.com
          oauth_login_endpoint: /oauth2/default/v1/token
          oauth_login_grant_type: refresh_token
          oauth_login_scope: kafka
          oauth_introspect_server: https://dev-1365.okta.com
          oauth_introspect_endpoint: /oauth2/default/v1/introspect
          oauth_sasl_mechanism: OAUTHBEARER
          oauth_security_protocol: SASL_PLAINTEXT
          oauth_sasl_login_callback_handler_class: org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler
          oauth_jwks_endpoint_url: https://dev-1365.okta.com/oauth2/default/v1/keys
  sink:
    - stdout:

```
