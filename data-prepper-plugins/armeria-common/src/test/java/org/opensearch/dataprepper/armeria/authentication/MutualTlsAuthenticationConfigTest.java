/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.armeria.authentication;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

class MutualTlsAuthenticationConfigTest {

    @TempDir
    static Path tempDir;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static MutualTlsAuthenticationConfig configWith(final String sslTrustCertificateFile) {
        return OBJECT_MAPPER.convertValue(
                Map.of("ssl_trust_certificate_file", sslTrustCertificateFile),
                MutualTlsAuthenticationConfig.class);
    }

    @Test
    void isTrustCertificateFileValid_true_for_a_readable_regular_file() throws Exception {
        final Path certFile = Files.writeString(tempDir.resolve("ca.crt"), "placeholder");
        final MutualTlsAuthenticationConfig config = configWith(certFile.toString());

        assertThat(config.isTrustCertificateFileValid(), is(true));
    }

    @Test
    void isTrustCertificateFileValid_false_when_path_is_a_directory() {
        final MutualTlsAuthenticationConfig config = configWith(tempDir.toString());

        assertThat(config.isTrustCertificateFileValid(), is(false));
    }

    @Test
    void isTrustCertificateFileValid_false_when_path_does_not_exist() {
        final MutualTlsAuthenticationConfig config = configWith("/nonexistent/path/ca.crt");

        assertThat(config.isTrustCertificateFileValid(), is(false));
    }

    @Test
    void isTrustCertificateFileValid_true_when_unset() {
        final MutualTlsAuthenticationConfig config = new MutualTlsAuthenticationConfig();

        assertThat(config.isTrustCertificateFileValid(), is(true));
    }
}
