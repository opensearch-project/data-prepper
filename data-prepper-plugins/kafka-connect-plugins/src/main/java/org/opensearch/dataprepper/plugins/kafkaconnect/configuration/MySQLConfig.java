/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.Connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class MySQLConfig extends ConnectorConfig {
    public static final String CONNECTOR_CLASS = "io.debezium.connector.mysql.MySqlConnector";
    private static final String SCHEMA_HISTORY_PRODUCER_PREFIX = "schema.history.internal.producer.";
    private static final String SCHEMA_HISTORY_CONSUMER_PREFIX = "schema.history.internal.consumer.";
    private static final String TOPIC_DEFAULT_PARTITIONS = "10";
    private static final String TOPIC_DEFAULT_REPLICATION_FACTOR = "-1";
    private static final String SCHEMA_HISTORY = "schemahistory";
    private static final String DEFAULT_SNAPSHOT_MODE = "initial";
    private static final String DEFAULT_PORT = "3306";

    @JsonProperty("hostname")
    @NotNull
    private String hostname;
    @JsonProperty("port")
    private String port = DEFAULT_PORT;
    @JsonProperty("credentials")
    private CredentialsConfig credentialsConfig;
    @JsonProperty("snapshot_mode")
    private String snapshotMode = DEFAULT_SNAPSHOT_MODE;
    @JsonProperty("tables")
    private List<TableConfig> tables = new ArrayList<>();

    @Override
    public List<Connector> buildConnectors() {
        return tables.stream().map(table -> {
            final String connectorName = table.getTopicPrefix() + "." + table.getTableName();
            final Map<String, String> config = buildConfig(table, connectorName);
            return new Connector(connectorName, config, this.forceUpdate);
        }).collect(Collectors.toList());
    }

    private Map<String, String> buildConfig(final TableConfig table, final String connectorName) {
        int databaseServerId = Math.abs(connectorName.hashCode());
        final Map<String, String> config = new HashMap<>();
        final Properties authProperties = this.getAuthProperties();
        if (authProperties != null) {
            authProperties.forEach((k, v) -> {
                if (k == WorkerConfig.BOOTSTRAP_SERVERS_CONFIG) {
                    this.setBootstrapServers(v.toString());
                    return;
                }
                if (v instanceof Class) {
                    config.put(SCHEMA_HISTORY_PRODUCER_PREFIX + k, ((Class<?>) v).getName());
                    config.put(SCHEMA_HISTORY_CONSUMER_PREFIX + k, ((Class<?>) v).getName());
                    return;
                }
                config.put(SCHEMA_HISTORY_PRODUCER_PREFIX + k, v.toString());
                config.put(SCHEMA_HISTORY_CONSUMER_PREFIX + k, v.toString());
            });
        }
        config.put("topic.creation.default.partitions", TOPIC_DEFAULT_PARTITIONS);
        config.put("topic.creation.default.replication.factor", TOPIC_DEFAULT_REPLICATION_FACTOR);
        config.put("connector.class", CONNECTOR_CLASS);
        config.put("database.hostname", hostname);
        config.put("database.port", port);
        config.put("database.user", credentialsConfig.getUsername());
        config.put("database.password", credentialsConfig.getPassword());
        config.put("snapshot.mode", snapshotMode);
        config.put("topic.prefix", table.getTopicPrefix());
        config.put("table.include.list", table.getTableName());
        config.put("schema.history.internal.kafka.bootstrap.servers", this.getBootstrapServers());
        config.put("schema.history.internal.kafka.topic", String.join(".", List.of(table.getTopicPrefix(), table.getTableName(), SCHEMA_HISTORY)));
        config.put("database.server.id", Integer.toString(databaseServerId));
        return config;
    }

    private static class TableConfig {
        @JsonProperty("topic_prefix")
        @NotNull
        private String topicPrefix;

        @JsonProperty("table_name")
        @NotNull
        private String tableName;

        public String getTableName() {
            return tableName;
        }

        public String getTopicPrefix() {
            return topicPrefix;
        }
    }
}
