# Kafka Connect Source

This is a source plugin that start a Kafka Connect and Connectors. Please note that the Kafka Connect Source has to work with Kafka Buffer.

## Usages
To get started with Kafka Connect source, 
config the kafka cluster in `data-prepper.config.yaml` configuration
```yaml
ssl: false
extensions:
  kafka_connect_config:
    worker_properties:
      client_id: client
      group_id: group
      config_storage_topic: pipeline-configs
      offset_storage_topic: pipeline-offsets
      status_storage_topic: pipeline-status
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
        asl:
        aws_msk_iam: default
```
for Local:
```yaml
ssl: false
extensions:
  kafka_connect_config:
    worker_properties:
      client_id: client
      group_id: group
      config_storage_topic: pipeline-configs
      offset_storage_topic: pipeline-offsets
      status_storage_topic: pipeline-status
  kafka_cluster_config:
    bootstrap_servers:
      - localhost:9092
    encryption:
      type: none
```
create the following `pipeline.yaml` configuration:
```yaml
mysql-pipeline:
  source:
    mysql:
    hostname: localhost
    credentials:
      plaintext:
        username: username
        password: password
    tables:
      - topic_prefix: prefix1
        table_name: dbname.tableName1
      - topic_prefix: prefix2
        table_name: dbname.tableName2
  sink:
  - noop:
          
mongodb-pipeline:
  mongodb:
    hostname: localhost
    credentials:
      plaintext:
        username: username
        password: password
    collections:
      - topic_prefix: prefix1
        collection_name: dbname.collection1
      - topic_prefix: prefix2
        collection_name: dbname.collection2
  sink:
    - noop:
        
postgres-pipeline:
  postgresql:
    hostname: localhost
    credentials:
      plaintext:
        username: username
        password: password
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
          serde_format: json
        - name: prefix2.dbname.collection2
          group_id: mongodb-group
          auto_offset_reset: earliest
          serde_format: json
        - name: prefix1.public.tableName1
          group_id: postgres-group
          auto_offset_reset: earliest
          serde_format: json
        - name: prefix1.dbname.tableName1
          group_id: mysql-group
          auto_offset_reset: earliest
          serde_format: json
        - name: prefix2.dbname.tableName2
          group_id: mysql-group
          auto_offset_reset: earliest
          serde_format: json
  sink:
    - stdout:
```


## Configurations

### Kafka Connect Config
| Options               | Required | Default | Type   | Description                                      |
|-----------------------|----------|---------|--------|--------------------------------------------------|
| worker_properties     | Yes      |         |        | Worker Properties                                |
| connect_timeout_ms    | No       | 60000   | Long   | The timeout used to detect if Kafka connect is running by data-prepper |
| connector_timeout_ms  | No       | 30000   | Long   | The timeout used to detect if Connectors are in a running state.   |

#### Worker Properties
| Option                       | Required | Default | Type   | Description                                                                                              |
|------------------------------|----------|---------|--------|----------------------------------------------------------------------------------------------------------|
| group_id                     | YES      |         | String | A unique string that identifies the connector clusters in OSI/OSDP node that this Worker belongs to.   |
| config_storage_topic         | YES      |         | String | The name of the topic where connector and task configuration data are stored.                           |
| offset_storage_topic         | YES      |         | String | The name of the topic where offsets are stored.                                                           |
| status_storage_topic         | YES      |         | String | The name of the topic where connectors and their tasks status are stored.                                 |
| client_id                    | NO       |         | String | An ID string to pass to the server when making requests.                                                    |
| offset_storage_partitions    | No       | 25      | int    | The number of partitions used when KafkaConnect creates the topic used to store connector offsets. Enter -1 to use the default number of partitions configured in the Kafka broker. |
| offset_flush_interval.ms     | No       | 60000   | Long   | Interval at which to try committing offsets for tasks. By default, connectors commit offsets per 60 secs. |
| offset_flush_timeout_ms     | No       | 5000    | Long   | Maximum number of milliseconds to wait for records to flush and partition offset data to be committed to offset storage before cancelling the process and restoring the offset data to be committed in a future attempt. |
| status_storage_partitions    | No       | 5       | int    | The number of partitions used when Connect creates the topic used to store connector and task status updates. Enter -1 to use the default number of partitions configured in the Kafka broker. |
| heartbeat_interval_ms        | No       | 3000    | Long   | Heartbeats are used to ensure that the Worker’s session stays active and to facilitate rebalancing when new members join or leave the group. |
| session_timeout_ms           | No       | 30000   | Long   | The timeout used to detect failures. Heartbeat must be sent to the broker before this time expires.        |

### MySQL
| Option          | Required | Default | Type         | Description                                                                                                         |
|-----------------|----------|---------|--------------|---------------------------------------------------------------------------------------------------------------------|
| hostname        | YES      |         | String       | The hostname of MySQL.                                                                                             |
| port            | NO       | 3306    | String       | The port of MySQL.                                                                                                 |
| snapshot_mode   | NO       | initial | String       | MySQL snapshot mode.                                                                                                |
| credentials     | YES      |         | Credentials  | The Credentials to access the database.                                                                             |
| tables          | YES      |         | List\<Table\> | The tables to ingest CDC data.                                                                                     |
| force_update    | NO       | FALSE   | Boolean      | When restarting or updating a pipeline, whether to force all connectors to update their config even if the connector name already exists. By default, if the connector name exists, the config will not be updated. The connector name is <topic_prefix>.<table_name>. |

Snapshot Mode [ref](https://debezium.io/documentation/reference/stable/connectors/mysql.html#mysql-property-snapshot-mode)

#### Table
| Option       | Required | Default | Type   | Description                                           |
|--------------|----------|---------|--------|-------------------------------------------------------|
| topic_prefix | YES      |         | String | Unique name that identifies the connector.            |
| table_name   | YES      |         | String | The table name to ingest, using *database.tableName* format. |

### Postgresql
| Option         | Required | Default  | Type         | Description                                                                                                         |
|----------------|----------|----------|--------------|---------------------------------------------------------------------------------------------------------------------|
| hostname       | YES      |          | String       | The hostname of MySQL.                                                                                             |
| port           | NO       | 5432     | String       | The port of MySQL.                                                                                                 |
| plugin_name    | NO       | pgoutput | ENUM         | The name of the PostgreSQL logical decoding plug-in installed on the PostgreSQL server. Supported values are `decoderbufs` and `pgoutput`. |
| snapshot_mode  | NO       | initial  | String       | PostgreSQL snapshot mode.                                                                                          |
| credentials    | YES      |          | Credentials  | The Credentials to access the database.                                                                             |
| tables         | YES      |          | List\<Table\> | The tables to ingest CDC data.                                                                                     |
| force_update   | NO       | FALSE    | Boolean      | When restarting or updating a pipeline, whether to force all connectors to update their config even if the connector name already exists. By default, if the connector name exists, the config will not be updated. The connector name is `<topic_prefix>.<table_name>`. |

Snapshot Mode [ref](https://debezium.io/documentation/reference/stable/connectors/postgresql.html#postgresql-property-snapshot-mode)

#### Table
| Option         | Required | Default | Type   | Description                                                                                                         |
|----------------|----------|---------|--------|---------------------------------------------------------------------------------------------------------------------|
| topic_prefix   | YES      |         | String | Unique name that identifies the connector.                                                                        |
| database_name  | YES      |         | String | The name of the PostgreSQL database from which to stream the changes.                                              |
| table_name     | YES      |         | String | The table name to ingest, using *schemaName.tableName* format.                                                     |

### MongoDB
| Option         | Required | Default       | Type               | Description                                                                                                                                                                                                                                                              |
|----------------|----------|---------------|--------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| hostname       | YES      |               | String             | The hostname of MySQL.                                                                                                                                                                                                                                                   |
| port           | NO       | 27017         | String             | The port of MySQL.                                                                                                                                                                                                                                                       |
| ssl            | NO       | FALSE         | Boolean            | Connector will use SSL to connect to MongoDB instances.                                                                                                                                                                                                                  |
| ingestion_mode | NO       | export_stream | String             | MongoDB ingestion mode. Available options: export_stream, stream, export                                                                                                                                                                                                 |
| export_config  | NO       |               | ExportConfig       | The Export Config                                                                                                                                                                                                                                           |
| credentials    | YES      |               | Credentials        | The Credentials to access the database.                                                                                                                                                                                                                                  |
| collections    | YES      |               | List\<Collection\> | The collections to ingest CDC data.                                                                                                                                                                                                                                      |
| force_update   | NO       | FALSE         | Boolean            | When restarting or updating a pipeline, whether to force all connectors to update their config even if the connector name already exists. By default, if the connector name exists, the config will not be updated. The connector name is `<topic_prefix>.<table_name>`. |
Snapshot Mode [ref](https://debezium.io/documentation/reference/stable/connectors/mongodb.html#mongodb-property-snapshot-mode)

#### ExportConfig
| Option              | Required   | Default            | Type    | Description                                                                                                                                                                                            |
|---------------------|------------|--------------------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| acknowledgments     | No         | FALSE              | Boolean | When true, enables the opensearch source to receive end-to-end acknowledgments when events are received by OpenSearch sinks. Default is false.                                                         |
| items_per_partition | No         | 4000               | Long    | Number of Items per partition during initial export.                                                                                                                                                   |
| read_preference     | No         | secondaryPreferred | String  | Operations typically read data from secondary members of the replica set. If the replica set has only one single primary member and no other members, operations read data from the primary member.    |


#### Collection

| Option           | Required | Default | Type   | Description                                                                                                         |
|------------------|----------|---------|--------|---------------------------------------------------------------------------------------------------------------------|
| topic_prefix     | YES      |         | String | Unique name that identifies the connector.                                                                        |
| collection_name  | YES      |         | String | The table name to ingest, using *database.collectionName* format.                                                     |


### Credentials
PlainText username and password
```yaml
credentials:
    plaintext:
      username: username
      password: password
```
Leverage AWS Secret Manager for username and password
```yaml
credentials:
  secret_manager:
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
- `source-record-write-total`: Number of records output from the transformations and written to Kafka for the task belonging to the named source connector in the worker (since the task was last restarted)
- `source-record-write-rate`: After transformations are applied, this is the average per-second number of records output from the transformations and written to Kafka for the task belonging to the named source connector in the worker (excludes any records filtered out by the transformations)
- `source-record-poll-total`: This is the number of records produced or polled by the task belonging to the named source connector (database.tableName) in the worker (since the task was last restarted)
- `source-record-poll-rate`:  This is the average per-second number of records produced or polled by the task belonging to the named source connector (database.tableName) in the worker
- `source-record-active-count-max`: Maximum number of records polled by the task but not yet completely written to Kafka
- `source-record-active-count-avg`: Average number of records polled by the task but not yet completely written to Kafka
- `source-record-active-count`: Most recent number of records polled by the task but not yet completely written to Kafka

## MongoDB Export Metric
MongoDB export has the following metrics:
- `exportRecordsSuccessTotal`: Number of records writes to the Buffer layer successfully.
- `exportRecordsFailedTotal`: Number of records failed to write to the Buffer layer.
- `exportPartitionSuccessTotal`: Number of partition been processed successfully
- `exportPartitionFailureTotal`: Number of partition failed to be processed.

# Developer Guide
This plugin is compatible with Java 14. See
- [CONTRIBUTING](https://github.com/opensearch-project/data-prepper/blob/main/CONTRIBUTING.md)
- [monitoring](https://github.com/opensearch-project/data-prepper/blob/main/docs/monitoring.md)
