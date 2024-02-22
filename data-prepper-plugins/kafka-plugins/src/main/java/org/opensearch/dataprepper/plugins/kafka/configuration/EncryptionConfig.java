/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EncryptionConfig {
    @JsonProperty("type")
    private EncryptionType type = EncryptionType.SSL;

    @JsonProperty("certificateContent")
    private String certificateContent;

    @JsonProperty("trustStoreFilePath")
    private String trustStoreFilePath;

    @JsonProperty("trustStorePassword")
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
