/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.armeria.authentication;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;

import java.nio.file.Files;
import java.nio.file.Path;

public class MutualTlsAuthenticationConfig {

    @JsonProperty("ssl_trust_certificate_file")
    @NotBlank(message = "ssl_trust_certificate_file is required for mutual_tls authentication")
    private String sslTrustCertificateFile;

    public String getSslTrustCertificateFile() {
        return sslTrustCertificateFile;
    }

    @AssertTrue(message = "ssl_trust_certificate_file must exist and be readable")
    boolean isTrustCertificateFileValid() {
        if (sslTrustCertificateFile == null || sslTrustCertificateFile.isBlank()) {
            return true;
        }
        Path path = Path.of(sslTrustCertificateFile);
        return Files.exists(path) && Files.isReadable(path);
    }
}
