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

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import org.opensearch.dataprepper.model.types.ByteCount;

public class ShuffleConfig {

    static final int DEFAULT_PARTITIONS = 64;
    static final String DEFAULT_TARGET_PARTITION_SIZE = "64mb";
    static final int DEFAULT_SERVER_PORT = 4995;

    @JsonProperty("partitions")
    @Min(1)
    @Max(10000)
    private int partitions = DEFAULT_PARTITIONS;

    @JsonProperty("target_partition_size")
    private ByteCount targetPartitionSize = ByteCount.parse(DEFAULT_TARGET_PARTITION_SIZE);

    @JsonProperty("server_port")
    private int serverPort = DEFAULT_SERVER_PORT;

    @JsonProperty("ssl")
    private boolean ssl = true;

    @JsonProperty("ssl_certificate_file")
    private String sslCertificateFile;

    @JsonProperty("ssl_key_file")
    private String sslKeyFile;

    @JsonProperty("ssl_insecure_disable_verification")
    private boolean sslInsecureDisableVerification = false;

    @AssertTrue(message = "ssl_certificate_file must be set when ssl is enabled")
    boolean isSslCertificateFileValid() {
        if (!ssl) {
            return true;
        }
        return sslCertificateFile != null && !sslCertificateFile.isEmpty();
    }

    @AssertTrue(message = "ssl_key_file must be set when ssl is enabled")
    boolean isSslKeyFileValid() {
        if (!ssl) {
            return true;
        }
        return sslKeyFile != null && !sslKeyFile.isEmpty();
    }

    public int getPartitions() { return partitions; }

    public long getTargetPartitionSizeBytes() { return targetPartitionSize.getBytes(); }

    public int getServerPort() { return serverPort; }

    public boolean isSsl() { return ssl; }

    public String getSslCertificateFile() { return sslCertificateFile; }

    public String getSslKeyFile() { return sslKeyFile; }

    public boolean isSslInsecureDisableVerification() { return sslInsecureDisableVerification; }
}
