/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.source.rds.configuration.AwsAuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;
import org.opensearch.dataprepper.plugins.source.rds.configuration.ExportConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.StreamConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.TlsConfig;
import software.amazon.awssdk.regions.Region;

import java.time.Duration;
import java.util.List;

/**
 * Configuration for RDS Source
 */
public class RdsSourceConfig {
    private static final int DEFAULT_S3_FOLDER_PARTITION_COUNT = 100;

    /**
     * Identifier for RDS instance/cluster or Aurora cluster
     */
    @JsonProperty("db_identifier")
    private String dbIdentifier;

    /**
     * Whether the db_identifier refers to a cluster or an instance
     */
    @JsonProperty("cluster")
    private boolean isCluster = false;

    @JsonProperty("engine")
    private EngineType engine = EngineType.MYSQL;

    /**
     * Whether the source is an Aurora cluster
     */
    @JsonProperty("aurora")
    private boolean isAurora = false;

    /**
     * The table name is in the format of `database.table` for MySQL engine
     */
    @JsonProperty("table_names")
    private List<String> tableNames;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationConfig awsAuthenticationConfig;

    @JsonProperty("acknowledgments")
    private boolean acknowledgments = false;

    @JsonProperty("s3_data_file_acknowledgment_timeout")
    private Duration dataFileAcknowledgmentTimeout = Duration.ofMinutes(30);

    @JsonProperty("stream_acknowledgment_timeout")
    private Duration streamAcknowledgmentTimeout = Duration.ofMinutes(10);

    @JsonProperty("s3_bucket")
    private String s3Bucket;

    @JsonProperty("s3_prefix")
    private String s3Prefix;

    @JsonProperty("s3_region")
    private String s3Region;

    /**
     * The number of folder partitions in S3 buffer
     */
    @JsonProperty("partition_count")
    @Min(1)
    @Max(1000)
    private int s3FolderPartitionCount = DEFAULT_S3_FOLDER_PARTITION_COUNT;

    @JsonProperty("export")
    @Valid
    private ExportConfig exportConfig;

    @JsonProperty("stream")
    private StreamConfig streamConfig;

    @JsonProperty("authentication")
    private AuthenticationConfig authenticationConfig;

    @JsonProperty("tls")
    private TlsConfig tlsConfig;

    @JsonProperty("disable_s3_read_for_leader")
    private boolean disableS3ReadForLeader = false;

    public String getDbIdentifier() {
        return dbIdentifier;
    }

    public boolean isCluster() {
        return isCluster || isAurora;
    }

    public EngineType getEngine() {
        return engine;
    }

    public boolean isAurora() {
        return isAurora;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public AwsAuthenticationConfig getAwsAuthenticationConfig() {
        return awsAuthenticationConfig;
    }

    public boolean isAcknowledgmentsEnabled() {
        return acknowledgments;
    }

    public Duration getDataFileAcknowledgmentTimeout() {
        return dataFileAcknowledgmentTimeout;
    }

    public Duration getStreamAcknowledgmentTimeout() {
        return streamAcknowledgmentTimeout;
    }

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getS3Prefix() {
        return s3Prefix;
    }

    public Region getS3Region() {
        return s3Region != null ? Region.of(s3Region) : null;
    }

    public int getPartitionCount() {
        return s3FolderPartitionCount;
    }

    public ExportConfig getExport() {
        return exportConfig;
    }

    public boolean isExportEnabled() {
        return exportConfig != null;
    }

    public StreamConfig getStream() {
        return streamConfig;
    }

    public boolean isStreamEnabled() {
        return streamConfig != null;
    }

    public TlsConfig getTlsConfig() {
        return tlsConfig;
    }

    public boolean isTlsEnabled() {
        return tlsConfig == null || !tlsConfig.isInsecure();
    }

    public boolean isDisableS3ReadForLeader() {
        return disableS3ReadForLeader;
    }

    public AuthenticationConfig getAuthenticationConfig() {
        return this.authenticationConfig;
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
