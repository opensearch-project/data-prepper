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
    private static class CollectionConfig {
        @JsonProperty("topic_prefix")
        @NotNull
        private String topicPrefix;

        @JsonProperty("collection_name")
        @NotNull
        private String collectionName;

        public String getCollectionName() {
            return collectionName;
        }

        public String getTopicPrefix() {
            return topicPrefix;
        }
    }
    private static final String MONGODB_CONNECTION_STRING_FORMAT = "mongodb://%s:%s/?replicaSet=rs0&directConnection=true";
    private static final String DEFAULT_PORT = "27017";
    private static final String DEFAULT_SNAPSHOT_MODE = "initial";

    private static final Boolean SSL_ENABLED = false;

    @JsonProperty("hostname")
    @NotNull
    private String hostname;

    @JsonProperty("port")
    private String port = DEFAULT_PORT;

    @JsonProperty("credentials")
    private CredentialsConfig credentialsConfig;

    @JsonProperty("snapshot_mode")
    private String snapshotMode = DEFAULT_SNAPSHOT_MODE;

    @JsonProperty("collections")
    private List<CollectionConfig> collections = new ArrayList<>();

    @JsonProperty("ssl")
    private Boolean ssl = SSL_ENABLED;

    @Override
    public List<Connector> buildConnectors() {
        return collections.stream().map(collection -> {
            final String connectorName = collection.getTopicPrefix() + "." + collection.getCollectionName();
            final Map<String, String> config = buildConfig(collection);
            return new Connector(connectorName, config, this.forceUpdate);
        }).collect(Collectors.toList());
    }

    private Map<String, String> buildConfig(final CollectionConfig collection) {
        Map<String, String> config = new HashMap<>();
        config.put("connector.class", CONNECTOR_CLASS);
        config.put("mongodb.connection.string", String.format(MONGODB_CONNECTION_STRING_FORMAT, hostname, port));
        config.put("mongodb.user", credentialsConfig.getUsername());
        config.put("mongodb.password", credentialsConfig.getPassword());
        config.put("snapshot.mode", snapshotMode);
        config.put("topic.prefix", collection.getTopicPrefix());
        config.put("collection.include.list", collection.getCollectionName());
        config.put("mongodb.ssl.enabled", ssl.toString());
        return config;
    }
}
