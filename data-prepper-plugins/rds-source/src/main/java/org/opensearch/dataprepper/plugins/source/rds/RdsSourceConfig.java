/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.source.rds.configuration.AwsAuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;
import org.opensearch.dataprepper.plugins.source.rds.configuration.ScanMode;
import org.opensearch.dataprepper.plugins.source.rds.configuration.ExportConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.JoinConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.TableFilterConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.TlsConfig;
import software.amazon.awssdk.regions.Region;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Configuration for RDS Source
 */
public class RdsSourceConfig {
    private static final int DEFAULT_S3_FOLDER_PARTITION_COUNT = 100;
    private static final Duration MAX_EMPTY_SCAN_DELAY = Duration.ofMinutes(30);

    /**
     * Identifier for RDS instance/cluster or Aurora cluster
     */
    @JsonProperty("db_identifier")
    @NotBlank
    private String dbIdentifier;

    /**
     * Whether the db_identifier refers to a cluster or an instance
     */
    @JsonProperty("cluster")
    private boolean isCluster = false;

    @JsonProperty("engine")
    @NotNull
    private EngineType engine;

    @JsonProperty("database")
    @NotEmpty
    private String database;

    @JsonProperty("tables")
    private TableFilterConfig tableFilterConfig;

    @JsonProperty("aws")
    @NotNull
    @Valid
    private AwsAuthenticationConfig awsAuthenticationConfig;

    @JsonProperty("acknowledgments")
    private boolean acknowledgments = true;

    @JsonProperty("s3_data_file_acknowledgment_timeout")
    private Duration dataFileAcknowledgmentTimeout = Duration.ofMinutes(30);

    @JsonProperty("stream_acknowledgment_timeout")
    private Duration streamAcknowledgmentTimeout = Duration.ofMinutes(10);

    @JsonProperty("s3_bucket")
    @NotBlank
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
    private boolean enableStream;

    @JsonProperty("authentication")
    @NotNull
    private AuthenticationConfig authenticationConfig;

    @JsonProperty("tls")
    private TlsConfig tlsConfig;

    @JsonProperty("disable_s3_read_for_leader")
    private boolean disableS3ReadForLeader = false;

    @JsonProperty("scan_mode")
    private ScanMode scanMode = ScanMode.IDLE_OPTIMIZED;

    /**
     * Minimum time to wait before an S3 buffer folder that had no objects is listed again.
     * Increasing this value lowers the number of S3 ListObjectsV2 calls made while the source is quiet,
     * at the cost of up to this much additional end-to-end latency. Defaults to zero (no delay).
     */
    @JsonProperty("empty_scan_delay")
    private Duration emptyScanDelay = Duration.ZERO;

    @JsonProperty("joins")
    @Valid
    private JoinConfig joinConfig;

    public String getDbIdentifier() {
        return dbIdentifier;
    }

    public boolean isCluster() {
        return isCluster || isAurora();
    }

    public EngineType getEngine() {
        return engine;
    }

    public boolean isAurora() {
        return engine.isAurora();
    }

    public String getDatabase() {
        return database;
    }

    public TableFilterConfig getTables() {
        return tableFilterConfig;
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

    public boolean isStreamEnabled() {
        return enableStream;
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

    public JoinConfig getJoinConfig() {
        return joinConfig;
    }

    public ScanMode getScanMode() {
        return this.scanMode;
    }

    public Duration getEmptyScanDelay() {
        return emptyScanDelay != null ? emptyScanDelay : Duration.ZERO;
    }

    @AssertTrue(message = "empty_scan_delay must be between 0 seconds and 30 minutes")
    boolean isEmptyScanDelayValid() {
        final Duration delay = getEmptyScanDelay();
        return !delay.isNegative() && delay.compareTo(MAX_EMPTY_SCAN_DELAY) <= 0;
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

    /**
     * This method applies the table filter configuration to the given set of table names.
     *
     * @param tableNames        The set of table names to be filtered
     */
    public void applyTableFilter(Set<String> tableNames) {
        if (tableFilterConfig == null) {
            return;
        }

        if (!tableFilterConfig.getInclude().isEmpty()) {
            List<String> includeTableList = tableFilterConfig.getInclude().stream()
                    .map(item -> getDatabase() + "." + item)
                    .collect(Collectors.toList());
            tableNames.retainAll(includeTableList);
        }

        if (!tableFilterConfig.getExclude().isEmpty()) {
            List<String> excludeTableList = tableFilterConfig.getExclude().stream()
                    .map(item -> getDatabase() + "." + item)
                    .collect(Collectors.toList());
            excludeTableList.forEach(tableNames::remove);
        }
    }
}
