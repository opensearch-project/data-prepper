/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EncryptionConfig {
    @JsonProperty("type")
    private EncryptionType type = EncryptionType.SSL;

    @JsonProperty("certificate_content")
    private String certificateContent;

    @JsonProperty("trust_store_file_path")
    private String trustStoreFilePath;

    @JsonProperty("trust_store_password")
    private String trustStorePassword;

    @JsonProperty("insecure")
    private boolean insecure = false;

    public EncryptionType getType() {
        return type;
    }

    public String getCertificateContent() {
        return certificateContent;
    }

    public String getTrustStoreFilePath() {
        return trustStoreFilePath;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public boolean getInsecure() {
        return insecure;
    }
}
