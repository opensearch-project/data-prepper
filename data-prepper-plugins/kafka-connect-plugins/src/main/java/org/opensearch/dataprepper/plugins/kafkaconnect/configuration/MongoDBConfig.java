/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafkaconnect.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.kafkaconnect.util.Connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MongoDBConfig extends ConnectorConfig {
    public static final String CONNECTOR_CLASS = "io.debezium.connector.mongodb.MongoDbConnector";
    private static final String MONGODB_CONNECTION_STRING_FORMAT = "mongodb://%s:%s/?replicaSet=rs0&directConnection=true";
    private static final String DEFAULT_PORT = "27017";
    private static final String DEFAULT_SNAPSHOT_MODE = "initial";
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
    @JsonProperty("snapshot_mode")
    private String snapshotMode = DEFAULT_SNAPSHOT_MODE;
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

    public String getSnapshotMode() {
        return this.snapshotMode;
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

    private Map<String, String> buildConfig(final CollectionConfig collection) {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", CONNECTOR_CLASS);
        config.put("mongodb.connection.string", String.format(MONGODB_CONNECTION_STRING_FORMAT, hostname, port));
        config.put("mongodb.user", credentialsConfig.getUsername());
        config.put("mongodb.password", credentialsConfig.getPassword());
        config.put("snapshot.mode", "never");
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
}
