/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import io.netty.handler.ssl.ClientAuth;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.dataprepper.armeria.authentication.ClientAuthConfiguration;
import org.opensearch.dataprepper.armeria.authentication.MutualTlsAuthenticationConfig;

import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MutualTlsArmeriaHttpAuthenticationProviderTest {

    @TempDir
    static Path tempDir;

    private static Path caCertFile;

    @BeforeAll
    static void generateCerts() throws Exception {
        KeyPair caKeyPair = TestCertificateGenerator.generateKeyPair();
        X509Certificate caCert = TestCertificateGenerator.generateCaCertificate(caKeyPair);
        caCertFile = TestCertificateGenerator.writeCertificateToPem(caCert, tempDir.resolve("ca.crt"));
    }

    @Test
    void constructor_throws_when_config_is_null() {
        assertThrows(NullPointerException.class, () -> new MutualTlsArmeriaHttpAuthenticationProvider(null));
    }

    @Test
    void constructor_throws_when_trust_cert_file_is_null() {
        MutualTlsAuthenticationConfig config = mock(MutualTlsAuthenticationConfig.class);
        when(config.getSslTrustCertificateFile()).thenReturn(null);
        assertThrows(NullPointerException.class, () -> new MutualTlsArmeriaHttpAuthenticationProvider(config));
    }

    @Test
    void constructor_throws_when_trust_cert_file_does_not_exist() {
        MutualTlsAuthenticationConfig config = mock(MutualTlsAuthenticationConfig.class);
        when(config.getSslTrustCertificateFile()).thenReturn("/nonexistent/path/ca.crt");
        assertThrows(IllegalArgumentException.class, () -> new MutualTlsArmeriaHttpAuthenticationProvider(config));
    }

    @Test
    void getClientAuthConfiguration_returns_non_empty() {
        MutualTlsAuthenticationConfig config = mock(MutualTlsAuthenticationConfig.class);
        when(config.getSslTrustCertificateFile()).thenReturn(caCertFile.toString());

        MutualTlsArmeriaHttpAuthenticationProvider provider = new MutualTlsArmeriaHttpAuthenticationProvider(config);

        assertThat(provider.getClientAuthConfiguration().isPresent(), is(true));
    }

    @Test
    void getClientAuthConfiguration_returns_client_auth_require() {
        MutualTlsAuthenticationConfig config = mock(MutualTlsAuthenticationConfig.class);
        when(config.getSslTrustCertificateFile()).thenReturn(caCertFile.toString());

        MutualTlsArmeriaHttpAuthenticationProvider provider = new MutualTlsArmeriaHttpAuthenticationProvider(config);
        Optional<ClientAuthConfiguration> clientAuthConfig = provider.getClientAuthConfiguration();

        assertThat(clientAuthConfig.get().getClientAuth(), equalTo(ClientAuth.REQUIRE));
        assertThat(clientAuthConfig.get().getTrustManagerFactory(), is(notNullValue()));
    }

    @Test
    void getAuthenticationDecorator_returns_empty() {
        MutualTlsAuthenticationConfig config = mock(MutualTlsAuthenticationConfig.class);
        when(config.getSslTrustCertificateFile()).thenReturn(caCertFile.toString());

        MutualTlsArmeriaHttpAuthenticationProvider provider = new MutualTlsArmeriaHttpAuthenticationProvider(config);

        assertThat(provider.getAuthenticationDecorator().isEmpty(), is(true));
    }
}
