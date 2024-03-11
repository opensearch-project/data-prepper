package org.opensearch.dataprepper.plugins.mongo.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;

public class MongoDBSourceConfig {
    private static final int DEFAULT_PORT = 27017;
    private static final Boolean SSL_ENABLED = false;
    private static final Boolean SSL_INVALID_HOST_ALLOWED = false;
    private static final String DEFAULT_SNAPSHOT_FETCH_SIZE = "1000";
    private static final String DEFAULT_READ_PREFERENCE = "secondaryPreferred";
    @JsonProperty("hostname")
    private @NotNull String hostname;
    @JsonProperty("port")
    private int port = DEFAULT_PORT;
    @JsonProperty("trust_store_file_path")
    private String trustStoreFilePath;
    @JsonProperty("trust_store_password")
    private String trustStorePassword;
    @JsonProperty("credentials")
    private CredentialsConfig credentialsConfig;

    @JsonProperty("snapshot_fetch_size")
    private String snapshotFetchSize;
    @JsonProperty("read_preference")
    private String readPreference;
    @JsonProperty("collections")
    private List<CollectionConfig> collections;
    @JsonProperty("acknowledgments")
    private Boolean acknowledgments = false;
    @JsonProperty("ssl")
    private Boolean ssl;
    @JsonProperty("ssl_invalid_host_allowed")
    private Boolean sslInvalidHostAllowed;
    @JsonProperty("direct_connection")
    private Boolean directConnection;

    public MongoDBSourceConfig() {
        this.snapshotFetchSize = DEFAULT_SNAPSHOT_FETCH_SIZE;
        this.readPreference = DEFAULT_READ_PREFERENCE;
        this.collections = new ArrayList<>();
        this.ssl = SSL_ENABLED;
        this.sslInvalidHostAllowed = SSL_INVALID_HOST_ALLOWED;
    }

    public CredentialsConfig getCredentialsConfig() {
        return this.credentialsConfig;
    }

    public String getHostname() {
        return this.hostname;
    }

    public int getPort() {
        return this.port;
    }

    public String getTrustStoreFilePath() {
        return this.trustStoreFilePath;
    }

    public String getTrustStorePassword() {
        return this.trustStorePassword;
    }

    public Boolean getSSLEnabled() {
        return this.ssl;
    }

    public Boolean getSSLInvalidHostAllowed() {
        return this.sslInvalidHostAllowed;
    }

    public Boolean getDirectConnection() {
        return this.directConnection;
    }

    public List<CollectionConfig> getCollections() {
        return this.collections;
    }

    public String getReadPreference() {
        return this.readPreference;
    }

    public boolean isAcknowledgmentsEnabled() {
        return this.acknowledgments;
    }

    public static class CredentialsConfig {
        @JsonProperty("user_name")
        private String userName;

        @JsonProperty("password")
        private String password;

        public String getUserName() {
            return userName;
        }

        public String getPassword() {
            return password;
        }

    }
}
