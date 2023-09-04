# Kafka Connect Source

This is a source plugin that start a Kafka Connect and Connectors. Please note that the Kafka Connect Source has to work with Kafka Buffer.

## Usages
To get started with Kafka Connect source, 
config the kafka cluster in `data-prepper.config.yaml` configuration
```yaml
ssl: false
kafka_cluster_config:
  encryption:
    type: ssl
    insecure: true
  aws:
    region: "us-east-1"
    msk:
      broker_connection_type: public
      arn: "msk-arn"
  authentication:
    sasl:
      aws_msk_iam: default
```
for Local:
```yaml
ssl: false
kafka_cluster_config:
  bootstrap_servers:
    - localhost:9092
  encryption:
    type: none
```
create the following `pipeline.yaml` configuration:
```yaml
connect-pipeline:
  source:
    kafka_connect:
      worker_properties:
        group_id: group
        config_storage_topic: pipeline-configs
        offset_storage_topic: pipeline-offsets
        status_storage_topic: pipeline-status
      mongodb:
        hostname: localhost
        credentials:
          type: plaintext
          username: username
          password: password
        collections:
          - topic_prefix: prefix1
            collection_name: dbname.collection1
          - topic_prefix: prefix2
            collection_name: dbname.collection2
      mysql:
        hostname: localhost
        credentials:
          type: plaintext
          username: username
          password: password
        tables:
          - topic_prefix: prefix1
            table_name: dbname.tableName1
          - topic_prefix: prefix2
            table_name: dbname.tableName2
      postgresql:
        hostname: localhost
        credentials:
          type: aws
          region: us-east-1
          secretId: secretId
        tables:
          - topic_prefix: prefix1
            database_name: dbname
            table_name: public.tableName1
  sink:
    - noop:

sink-pipeline:
  source:
    kafka:
      topics:
        - name: prefix1.dbname.collection1
          group_id: mongodb-group
          auto_offset_reset: earliest
        - name: prefix2.dbname.collection2
          group_id: mongodb-group
          auto_offset_reset: earliest
        - name: prefix1.public.tableName1
          group_id: postgres-group
          auto_offset_reset: earliest
        - name: prefix1.dbname.tableName1
          group_id: mysql-group
          auto_offset_reset: earliest
        - name: prefix2.dbname.tableName2
          group_id: mysql-group
          auto_offset_reset: earliest
  sink:
    - stdout:
```


## Configurations

### Worker Property
```yaml
  worker_properties:
    group_id: test-group #required
    config_storage_topic: test-configs #required
    offset_storage_topic: test-offsets #required
    status_storage_topic: test-status #required
    key_converter: org.apache.kafka.connect.json.JsonConverter #optional default is org.apache.kafka.connect.json.JsonConverter
    key_converter_schemas_enable: false #optional and default is false
    key_converter_schema_registry_url: http://localhost:8081/ #optional
    value_converter: org.apache.kafka.connect.json.JsonConverter #optional default is org.apache.kafka.connect.json.JsonConverter
    value_converter_schemas_enable: false #optional and default is false
    value_converter_schema_registry_url: http://localhost:8082/ #optional
    offset_storage_partitions: 25 #optional and default is 25
    offset_flush_interval_ms: 60000 #optional and default is 60000 (60s)
    offset_flush_timeout_ms: 5000 #optional and default is 5000 (5s)
    status_storage_partitions: 5 #optional and default is 5
    heartbeat_interval_ms: 3000 #optional and default is 3000 (3s)
    session_timeout_ms: 30000 #optional and default is 30000 (30s)
```

### Connectors
Only supports `mysql`, `postgresql` and `mongodb`
```yaml
  mysql:
    hostname: localhost # required
    snapshot_mode: initial # Optional
    force_update: false # Optional
    credentials: # Read Credential Section for detailed information
      type: plaintext
      username: username
      password: password
    tables:
      - topic_prefix: prefix1 # required the topic_prefix need to be unique among MySQL connectors
        table_name: dbname.tableName1 # required databaseName.tableName`
      - topic_prefix: prefix2
        table_name: dbname.tableName2
```
Note:
Every table is treat as a connector.

### Credentials
PlainText username and password
```yaml
credentials:
  type: plaintext #required one of plaintext/aws
  username: username
  password: password
```
Leverage AWS Secret Manager for username and password
```yaml
credentials:
    type: aws #required one of plaintext/aws
    region: us-east-1 #required aws region
    secretId: secretId #required secret manager secretId
    sts_role_arn: sts-role #optional
```
Note:
* `sts_role_arn` (Optional) : The AWS STS role to assume for requests to Secret Manager. Defaults to null, which will use the [standard SDK behavior for credentials](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/credentials.html).

### Kafka Cluster Configuration

The Kafka Cluster Configuration must be set in data-prepper's configuration yaml.

```yaml
kafka_cluster_config:
  bootstrap_servers:
    - localhost:9092
  encryption:
    type: ssl
    insecure: true
  aws:
    sts_role_arn: sts-role-arn
    region: us-east-1
    msk:
      broker_connection_type: public
      arn: msk-arn
  authentication:
    sasl:
      aws_msk_iam: default
```
* `bootstrap_servers` (Optional) Required if `aws` and `msk` is not configured.
* `encryption` (Required) https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/sources/kafka/#encryption
* `aws` (Optional) https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/sources/kafka/#aws
* `authentication` (Optional) https://opensearch.org/docs/latest/data-prepper/pipelines/configuration/sources/kafka/#authentication

# Metrics

## Kafka Connect Metrics
Kafka Connect Worker Metrics

- `task-count`: Number of tasks that have run in this worker
- `connector-count`: The number of connectors that have run in this worker
- `connector-startup-attempts-total`: Total number of connector startups that this worker has attempted
- `connector-startup-success-total`: Total number of connector starts that succeeded
- `connector-startup-failure-total`: Total number of connector starts that failed
- `task-startup-attempts-total`: Total number of task startups that the worker has attempted
- `task-startup-success-total`: Total number of task starts that succeeded
- `task-startup-failure-total`: Total number of task starts that failed

## Connector Metrics
Each connector contains following metrics:

- `source-record-poll-total`: This is the number of records produced or polled by the task belonging to the named source connector (database.tableName) in the worker (since the task was last restarted)
- `source-record-poll-rate`:  This is the average per-second number of records produced or polled by the task belonging to the named source connector (database.tableName) in the worker
- `source-record-active-count-max`: Maximum number of records polled by the task but not yet completely written to Kafka
- `source-record-active-count-avg`: Average number of records polled by the task but not yet completely written to Kafka
- `source-record-active-count`: Most recent number of records polled by the task but not yet completely written to Kafka

## Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
