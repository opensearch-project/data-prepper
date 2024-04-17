/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;

public class EncryptionConfig {
    @JsonProperty("type")
    private EncryptionType type = EncryptionType.SSL;

    @JsonAlias("certificate_content")
    @JsonProperty("certificate_key")
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
