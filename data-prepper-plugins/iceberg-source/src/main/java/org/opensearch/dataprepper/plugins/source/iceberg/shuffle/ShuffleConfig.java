/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.iceberg.shuffle;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.opensearch.dataprepper.http.BaseHttpServerConfig;
import org.opensearch.dataprepper.model.types.ByteCount;

// Node-to-node authentication uses mutual TLS (ssl_client_auth) instead of
// plugin-based authentication, so the inherited "authentication" field is not used.
@JsonIgnoreProperties({"path", "compression", "authentication", "health_check_service",
        "unauthenticated_health_check", "request_timeout", "thread_count",
        "max_connection_count", "max_pending_requests", "max_request_length"})
public class ShuffleConfig extends BaseHttpServerConfig {

    static final int DEFAULT_PARTITIONS = 64;
    static final String DEFAULT_TARGET_PARTITION_SIZE = "64mb";
    static final int DEFAULT_SERVER_PORT = 4995;

    @JsonProperty("partitions")
    @Min(1)
    @Max(10000)
    private int partitions = DEFAULT_PARTITIONS;

    @JsonProperty("target_partition_size")
    private ByteCount targetPartitionSize = ByteCount.parse(DEFAULT_TARGET_PARTITION_SIZE);

    @JsonProperty("storage_path")
    private String storagePath;

    @JsonProperty("ssl")
    private boolean ssl = true;

    @JsonProperty("ssl_client_auth")
    private boolean sslClientAuth = false;

    @JsonProperty("ssl_insecure_disable_verification")
    private boolean sslInsecureDisableVerification = false;

    @Override
    public int getDefaultPort() {
        return DEFAULT_SERVER_PORT;
    }

    @Override
    public String getDefaultPath() {
        return "/shuffle";
    }

    @Override
    public boolean isSsl() {
        return ssl;
    }

    @Override
    public boolean isSslCertAndKeyFileInS3() {
        return ssl && getSslCertificateFile() != null
                && getSslCertificateFile().toLowerCase().startsWith("s3://")
                && getSslKeyFile() != null
                && getSslKeyFile().toLowerCase().startsWith("s3://");
    }

    @Override
    public boolean isSslCertificateFileValid() {
        if (ssl && !isUseAcmCertificateForSsl()) {
            return getSslCertificateFile() != null && !getSslCertificateFile().isEmpty();
        }
        return true;
    }

    @Override
    public boolean isSslKeyFileValid() {
        if (ssl && !isUseAcmCertificateForSsl()) {
            return getSslKeyFile() != null && !getSslKeyFile().isEmpty();
        }
        return true;
    }

    public int getPartitions() { return partitions; }

    public long getTargetPartitionSizeBytes() { return targetPartitionSize.getBytes(); }

    public String getStoragePath() { return storagePath; }

    public int getServerPort() { return getPort(); }

    public boolean isSslInsecureDisableVerification() { return sslInsecureDisableVerification; }

    public boolean isSslClientAuth() { return sslClientAuth; }
}
