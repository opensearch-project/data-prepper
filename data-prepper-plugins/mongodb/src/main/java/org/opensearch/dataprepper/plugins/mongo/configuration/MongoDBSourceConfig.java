package org.opensearch.dataprepper.plugins.mongo.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class MongoDBSourceConfig {
    private static final int DEFAULT_PORT = 27017;
    private static final Boolean DEFAULT_INSECURE = false;
    private static final Boolean DEFAULT_INSECURE_DISABLE_VERIFICATION = false;
    private static final String DEFAULT_READ_PREFERENCE = "primaryPreferred";
    private static final Boolean DEFAULT_DIRECT_CONNECT = true;
    private static final Duration DEFAULT_ACKNOWLEDGEMENT_SET_TIMEOUT = Duration.ofHours(2);
    @JsonProperty("host")
    private @NotNull String host;
    @JsonProperty("port")
    private int port = DEFAULT_PORT;
    @JsonProperty("trust_store_file_path")
    private String trustStoreFilePath;
    @JsonProperty("trust_store_password")
    private String trustStorePassword;
    @JsonProperty("authentication")
    private AuthenticationConfig authenticationConfig;

    @JsonProperty("read_preference")
    private String readPreference;
    @JsonProperty("collections")
    @Valid
    @NotNull
    @Size(min = 1)
    private List<CollectionConfig> collections;
    @JsonProperty("acknowledgments")
    private Boolean acknowledgments = false;

    @JsonProperty("s3_bucket")
    private String s3Bucket;

    @JsonProperty("s3_prefix")
    private String s3Prefix;

    @JsonProperty("s3_region")
    private String s3Region;

    @JsonProperty
    private Duration partitionAcknowledgmentTimeout;

    @JsonProperty("insecure")
    private Boolean insecure;
    @JsonProperty("ssl_insecure_disable_verification")
    private Boolean sslInsecureDisableVerification;
    @JsonProperty("direct_connection")
    private Boolean directConnection;

    @JsonProperty("aws")
    @Valid
    private AwsConfig awsConfig;

    @JsonProperty("disable_s3_read_for_leader")
    private boolean disableS3ReadForLeader = false;

    @JsonProperty("id_key")
    private String idKey;

    public MongoDBSourceConfig() {
        this.readPreference = DEFAULT_READ_PREFERENCE;
        this.collections = new ArrayList<>();
        this.insecure = DEFAULT_INSECURE;
        this.sslInsecureDisableVerification = DEFAULT_INSECURE_DISABLE_VERIFICATION;
        this.directConnection = DEFAULT_DIRECT_CONNECT;
        this.partitionAcknowledgmentTimeout = DEFAULT_ACKNOWLEDGEMENT_SET_TIMEOUT;
    }

    public AuthenticationConfig getAuthenticationConfig() {
        return this.authenticationConfig;
    }

    public String getHost() {
        return this.host;
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

    public String getS3Bucket() {
        return this.s3Bucket;
    }

    public String getS3Prefix() {
        return this.s3Prefix;
    }

    public String getS3Region() {
        return this.s3Region;
    }

    public boolean isDisableS3ReadForLeader() {
        return disableS3ReadForLeader;
    }

    public String getIdKey() {
        return this.idKey;
    }

    public AwsConfig getAwsConfig() {
        return this.awsConfig;
    }

    public static class AuthenticationConfig {
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
