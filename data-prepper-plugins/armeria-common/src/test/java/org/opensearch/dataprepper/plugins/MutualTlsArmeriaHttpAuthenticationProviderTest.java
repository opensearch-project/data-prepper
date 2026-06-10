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
import io.netty.handler.ssl.SslContextBuilder;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.armeria.authentication.MutualTlsAuthenticationConfig;

import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MutualTlsArmeriaHttpAuthenticationProviderTest {

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
    void getTlsCustomizer_returns_non_empty() {
        MutualTlsAuthenticationConfig config = mock(MutualTlsAuthenticationConfig.class);
        when(config.getSslTrustCertificateFile()).thenReturn("/tmp/ca.crt");

        MutualTlsArmeriaHttpAuthenticationProvider provider = new MutualTlsArmeriaHttpAuthenticationProvider(config);

        assertTrue(provider.getTlsCustomizer().isPresent());
    }

    @Test
    void getTlsCustomizer_sets_client_auth_require() {
        MutualTlsAuthenticationConfig config = mock(MutualTlsAuthenticationConfig.class);
        when(config.getSslTrustCertificateFile()).thenReturn("/tmp/ca.crt");

        MutualTlsArmeriaHttpAuthenticationProvider provider = new MutualTlsArmeriaHttpAuthenticationProvider(config);
        Consumer<SslContextBuilder> customizer = provider.getTlsCustomizer().get();

        SslContextBuilder sslContextBuilder = mock(SslContextBuilder.class);
        when(sslContextBuilder.clientAuth(ClientAuth.REQUIRE)).thenReturn(sslContextBuilder);

        customizer.accept(sslContextBuilder);

        verify(sslContextBuilder).clientAuth(ClientAuth.REQUIRE);
    }

    @Test
    void getAuthenticationDecorator_returns_empty() {
        MutualTlsAuthenticationConfig config = mock(MutualTlsAuthenticationConfig.class);
        when(config.getSslTrustCertificateFile()).thenReturn("/tmp/ca.crt");

        MutualTlsArmeriaHttpAuthenticationProvider provider = new MutualTlsArmeriaHttpAuthenticationProvider(config);

        assertTrue(provider.getAuthenticationDecorator().isEmpty());
    }
}
