/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.configuration;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.Connector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class PostgreSQLConfig extends ConnectorConfig {
    public static final String CONNECTOR_CLASS = "io.debezium.connector.postgresql.PostgresConnector";
    private static final String TOPIC_DEFAULT_PARTITIONS = "10";
    private static final String TOPIC_DEFAULT_REPLICATION_FACTOR = "-1";
    private static final String DEFAULT_PORT = "5432";
    private static final String DEFAULT_SNAPSHOT_MODE = "initial";
    private static final PluginName DEFAULT_DECODING_PLUGIN = PluginName.PGOUTPUT; // default plugin for Aurora PostgreSQL
    @JsonProperty("hostname")
    @NotNull
    private String hostname;
    @JsonProperty("port")
    private String port = DEFAULT_PORT;
    /**
     * The name of the PostgreSQL logical decoding plug-in installed on the PostgreSQL server.
     * Supported values are decoderbufs, and pgoutput.
     */
    @JsonProperty("plugin_name")
    private PluginName pluginName = DEFAULT_DECODING_PLUGIN;
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
            final Map<String, String> config = buildConfig(table);
            return new Connector(connectorName, config, this.forceUpdate);
        }).collect(Collectors.toList());
    }

    private Map<String, String> buildConfig(final TableConfig tableName) {
        Map<String, String> config = new HashMap<>();
        config.put("topic.creation.default.partitions", TOPIC_DEFAULT_PARTITIONS);
        config.put("topic.creation.default.replication.factor", TOPIC_DEFAULT_REPLICATION_FACTOR);
        config.put("connector.class", CONNECTOR_CLASS);
        config.put("plugin.name", pluginName.type);
        config.put("database.hostname", hostname);
        config.put("database.port", port);
        config.put("database.user", credentialsConfig.getUsername());
        config.put("database.password", credentialsConfig.getPassword());
        config.put("snapshot.mode", snapshotMode);
        config.put("topic.prefix", tableName.getTopicPrefix());
        config.put("database.dbname", tableName.getDatabaseName());
        config.put("table.include.list", tableName.getTableName());
        // Non-configurable properties used to transform CDC data before sending to Kafka.
        config.put("transforms", "unwrap");
        config.put("transforms.unwrap.type", "io.debezium.transforms.ExtractNewRecordState");
        config.put("transforms.unwrap.drop.tombstones", "true");
        config.put("transforms.unwrap.delete.handling.mode", "rewrite");
        config.put("transforms.unwrap.add.fields", "op,table,source.ts_ms,source.db,source.snapshot,ts_ms");
        return config;
    }

    public enum PluginName {
        DECODERBUFS("decoderbufs"),
        PGOUTPUT("pgoutput");

        private static final Map<String, PostgreSQLConfig.PluginName> OPTIONS_MAP = Arrays.stream(PostgreSQLConfig.PluginName.values())
                .collect(Collectors.toMap(
                        value -> value.type,
                        value -> value
                ));

        private final String type;

        PluginName(final String type) {
            this.type = type;
        }

        @JsonCreator
        public static PostgreSQLConfig.PluginName fromTypeValue(final String type) {
            return OPTIONS_MAP.get(type.toLowerCase());
        }
    }

    private static class TableConfig {
        @JsonProperty("database")
        @NotNull
        private String databaseName;

        @JsonProperty("topic_prefix")
        @NotNull
        private String topicPrefix;

        @JsonProperty("table")
        @NotNull
        private String tableName;

        public String getDatabaseName() {
            return databaseName;
        }

        public String getTableName() {
            return tableName;
        }

        public String getTopicPrefix() {
            return topicPrefix;
        }
    }
}
