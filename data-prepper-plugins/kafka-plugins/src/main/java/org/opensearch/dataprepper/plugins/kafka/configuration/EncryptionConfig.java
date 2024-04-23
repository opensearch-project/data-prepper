/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.configuration;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import org.opensearch.dataprepper.plugins.certificate.validation.PemObjectValidator;

import java.nio.file.Paths;

public class EncryptionConfig {
    @JsonProperty("type")
    private EncryptionType type = EncryptionType.SSL;

    @JsonAlias("certificate_content")
    @JsonProperty("certificate")
    private String certificate;

    @JsonProperty("trust_store_file_path")
    private String trustStoreFilePath;

    @JsonProperty("trust_store_password")
    private String trustStorePassword;

    @JsonProperty("insecure")
    private boolean insecure = false;

    public EncryptionType getType() {
        return type;
    }

    public String getCertificate() {
        return certificate;
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

    @AssertTrue(message = "certificate must be either valid PEM file path or public key content.")
    boolean isCertificateValid() {
        if (PemObjectValidator.isPemObject(certificate)) {
            return true;
        }
        try {
            Paths.get(certificate);
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
