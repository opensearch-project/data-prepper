package org.opensearch.dataprepper.plugins.mongo.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class MongoDBSourceConfig {
    private static final int DEFAULT_PORT = 27017;
    private static final Boolean DEFAULT_INSECURE = false;
    private static final Boolean DEFAULT_INSECURE_DISABLE_VERIFICATION = false;
    private static final String DEFAULT_SNAPSHOT_FETCH_SIZE = "1000";
    private static final String DEFAULT_READ_PREFERENCE = "primaryPreferred";
    private static final Boolean DEFAULT_DIRECT_CONNECT = true;
    private static final Duration DEFAULT_ACKNOWLEDGEMENT_SET_TIMEOUT = Duration.ofHours(2);
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

    @JsonProperty
    private Duration partitionAcknowledgmentTimeout;

    @JsonProperty("insecure")
    private Boolean insecure;
    @JsonProperty("ssl_insecure_disable_verification")
    private Boolean sslInsecureDisableVerification;
    @JsonProperty("direct_connection")
    private Boolean directConnection;

    public MongoDBSourceConfig() {
        this.snapshotFetchSize = DEFAULT_SNAPSHOT_FETCH_SIZE;
        this.readPreference = DEFAULT_READ_PREFERENCE;
        this.collections = new ArrayList<>();
        this.insecure = DEFAULT_INSECURE;
        this.sslInsecureDisableVerification = DEFAULT_INSECURE_DISABLE_VERIFICATION;
        this.directConnection = DEFAULT_DIRECT_CONNECT;
        this.partitionAcknowledgmentTimeout = DEFAULT_ACKNOWLEDGEMENT_SET_TIMEOUT;
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

    public Boolean getTls() {
        return !this.insecure;
    }

    public Boolean getSslInsecureDisableVerification() {
        return this.sslInsecureDisableVerification;
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

    public Duration getPartitionAcknowledgmentTimeout() {
        return this.partitionAcknowledgmentTimeout;
    }

    public static class CredentialsConfig {
        @JsonProperty("username")
        private String username;

        @JsonProperty("password")
        private String password;

        public String getUsername() {
            return username;
        }

        public String getPassword() {
            return password;
        }

    }
}
