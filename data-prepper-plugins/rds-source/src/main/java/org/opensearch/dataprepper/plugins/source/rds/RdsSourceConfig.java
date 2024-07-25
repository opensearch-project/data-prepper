/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.rds;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.plugins.source.rds.configuration.AwsAuthenticationConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.EngineType;
import org.opensearch.dataprepper.plugins.source.rds.configuration.ExportConfig;
import org.opensearch.dataprepper.plugins.source.rds.configuration.StreamConfig;

import java.util.List;

/**
 * Configuration for RDS Source
 */
public class RdsSourceConfig {

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

    /**
     * S3 bucket for holding both export and stream data
     */
    @JsonProperty("s3_bucket")
    private String s3Bucket;

    @JsonProperty("s3_prefix")
    private String s3Prefix;

    @JsonProperty("s3_region")
    private String s3Region;

    @JsonProperty("export")
    @Valid
    private ExportConfig exportConfig;

    @JsonProperty("stream")
    private StreamConfig streamConfig;

    @JsonProperty("authentication")
    private AuthenticationConfig authenticationConfig;

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

    public String getS3Bucket() {
        return s3Bucket;
    }

    public String getS3Prefix() {
        return s3Prefix;
    }

    public String getS3Region() {
        return s3Region;
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
