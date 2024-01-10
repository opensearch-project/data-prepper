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

public class MongoDBConfig extends ConnectorConfig {
    public static final String CONNECTOR_CLASS = "io.debezium.connector.mongodb.MongoDbConnector";
    private static final String MONGODB_CONNECTION_STRING_FORMAT = "mongodb://%s:%s/?replicaSet=rs0&directConnection=true";
    private static final String DEFAULT_PORT = "27017";
    private static final String DEFAULT_SNAPSHOT_MODE = "never";
    private static final Boolean SSL_ENABLED = false;
    private static final Boolean SSL_INVALID_HOST_ALLOWED = false;
    private static final String DEFAULT_SNAPSHOT_FETCH_SIZE = "1000";
    @JsonProperty("hostname")
    @NotNull
    private String hostname;
    @JsonProperty("port")
    private String port = DEFAULT_PORT;
    @JsonProperty("credentials")
    private CredentialsConfig credentialsConfig;
    @JsonProperty("ingestion_mode")
    private IngestionMode ingestionMode = IngestionMode.EXPORT_STREAM;
    @JsonProperty("export_config")
    private ExportConfig exportConfig = new ExportConfig();
    @JsonProperty("snapshot_fetch_size")
    private String snapshotFetchSize = DEFAULT_SNAPSHOT_FETCH_SIZE;
    @JsonProperty("collections")
    private List<CollectionConfig> collections = new ArrayList<>();
    @JsonProperty("ssl")
    private Boolean ssl = SSL_ENABLED;
    @JsonProperty("ssl_invalid_host_allowed")
    private Boolean sslInvalidHostAllowed = SSL_INVALID_HOST_ALLOWED;

    @Override
    public List<Connector> buildConnectors() {
        return collections.stream().map(collection -> {
            final String connectorName = collection.getTopicPrefix() + "." + collection.getCollectionName();
            final Map<String, String> config = buildConfig(collection);
            return new Connector(connectorName, config, this.forceUpdate);
        }).collect(Collectors.toList());
    }

    public IngestionMode getIngestionMode() {
        return this.ingestionMode;
    }

    public CredentialsConfig getCredentialsConfig() {
        return this.credentialsConfig;
    }

    public String getHostname() {
        return this.hostname;
    }

    public String getPort() {
        return this.port;
    }

    public Boolean getSSLEnabled() {
        return this.ssl;
    }

    public Boolean getSSLInvalidHostAllowed() {
        return this.sslInvalidHostAllowed;
    }

    public List<CollectionConfig> getCollections() {
        return this.collections;
    }

    public ExportConfig getExportConfig() {
        return this.exportConfig;
    }

    private Map<String, String> buildConfig(final CollectionConfig collection) {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", CONNECTOR_CLASS);
        config.put("mongodb.connection.string", String.format(MONGODB_CONNECTION_STRING_FORMAT, hostname, port));
        config.put("mongodb.user", credentialsConfig.getUsername());
        config.put("mongodb.password", credentialsConfig.getPassword());
        config.put("snapshot.mode", DEFAULT_SNAPSHOT_MODE);
        config.put("snapshot.fetch.size", snapshotFetchSize);
        config.put("topic.prefix", collection.getTopicPrefix());
        config.put("collection.include.list", collection.getCollectionName());
        config.put("mongodb.ssl.enabled", ssl.toString());
        config.put("mongodb.ssl.invalid.hostname.allowed", sslInvalidHostAllowed.toString());
        // Non-configurable properties used to transform CDC data before sending to Kafka.
        config.put("transforms", "unwrap");
        config.put("transforms.unwrap.type", "io.debezium.connector.mongodb.transforms.ExtractNewDocumentState");
        config.put("transforms.unwrap.drop.tombstones", "true");
        config.put("transforms.unwrap.delete.handling.mode", "rewrite");
        config.put("transforms.unwrap.add.fields", "op,rs,collection,source.ts_ms,source.db,source.snapshot,ts_ms");
        return config;
    }

    public enum IngestionMode {
        EXPORT_STREAM("export_stream"),
        EXPORT("export"),
        STREAM("stream");

        private static final Map<String, IngestionMode> OPTIONS_MAP = Arrays.stream(IngestionMode.values())
                .collect(Collectors.toMap(
                        value -> value.type,
                        value -> value
                ));

        private final String type;

        IngestionMode(final String type) {
            this.type = type;
        }

        @JsonCreator
        public static IngestionMode fromTypeValue(final String type) {
            return OPTIONS_MAP.get(type.toLowerCase());
        }
    }

    public static class CollectionConfig {
        @JsonProperty("topic_prefix")
        @NotNull
        private String topicPrefix;

        @JsonProperty("collection")
        @NotNull
        private String collectionName;

        public String getCollectionName() {
            return collectionName;
        }

        public String getTopicPrefix() {
            return topicPrefix;
        }
    }

    public static class ExportConfig {
        private static int DEFAULT_ITEMS_PER_PARTITION = 4000;
        private static String DEFAULT_READ_PREFERENCE = "secondaryPreferred";
        @JsonProperty("acknowledgments")
        private Boolean acknowledgments = false;
        @JsonProperty("items_per_partition")
        private Integer itemsPerPartition = DEFAULT_ITEMS_PER_PARTITION;
        @JsonProperty("read_preference")
        private String readPreference = DEFAULT_READ_PREFERENCE;

        public boolean getAcknowledgements() {
            return this.acknowledgments;
        }

        public Integer getItemsPerPartition() {
            return this.itemsPerPartition;
        }

        public String getReadPreference() {
            return this.readPreference;
        }
    }
}
