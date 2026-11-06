/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.netty.handler.ssl.ClientAuth;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.dataprepper.armeria.authentication.ClientAuthConfiguration;
import org.opensearch.dataprepper.armeria.authentication.MutualTlsAuthenticationConfig;

import javax.net.ssl.SSLPeerUnverifiedException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
    void constructor_throws_when_trust_cert_file_is_a_directory() {
        final MutualTlsAuthenticationConfig config = mock(MutualTlsAuthenticationConfig.class);
        when(config.getSslTrustCertificateFile()).thenReturn(tempDir.toString());
        assertThrows(IllegalArgumentException.class, () -> new MutualTlsArmeriaHttpAuthenticationProvider(config));
    }

    @Test
    void constructor_throws_when_trust_cert_file_has_no_certificates() throws Exception {
        final Path emptyCertFile = Files.createFile(tempDir.resolve("empty-trust.crt"));

        final MutualTlsAuthenticationConfig config = mock(MutualTlsAuthenticationConfig.class);
        when(config.getSslTrustCertificateFile()).thenReturn(emptyCertFile.toString());

        final IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
                () -> new MutualTlsArmeriaHttpAuthenticationProvider(config));
        assertThat(exception.getMessage(), containsString("contains no certificates"));
    }

    @Test
    void buildTrustManagerFactory_loads_single_ca() throws Exception {
        final TrustManagerFactory trustManagerFactory = MutualTlsArmeriaHttpAuthenticationProvider
                .buildTrustManagerFactory(caCertFile.toString());

        assertThat(trustManagerFactory, is(notNullValue()));
        assertThat(((X509TrustManager) trustManagerFactory.getTrustManagers()[0]).getAcceptedIssuers().length, is(1));
    }

    @Test
    void buildTrustManagerFactory_loads_multiple_cas_from_one_pem() throws Exception {
        final KeyPair secondCaKeyPair = TestCertificateGenerator.generateKeyPair();
        final X509Certificate secondCa = TestCertificateGenerator.generateCaCertificate(secondCaKeyPair);
        final Path secondCaFile = TestCertificateGenerator.writeCertificateToPem(secondCa, tempDir.resolve("ca2.crt"));

        final Path concatenated = tempDir.resolve("ca-bundle.crt");
        Files.writeString(concatenated,
                Files.readString(caCertFile) + System.lineSeparator() + Files.readString(secondCaFile));

        final TrustManagerFactory trustManagerFactory = MutualTlsArmeriaHttpAuthenticationProvider
                .buildTrustManagerFactory(concatenated.toString());

        assertThat(trustManagerFactory, is(notNullValue()));
        assertThat(((X509TrustManager) trustManagerFactory.getTrustManagers()[0]).getAcceptedIssuers().length, is(2));
    }

    @Test
    void buildTrustManagerFactory_throws_when_file_is_not_a_certificate() throws Exception {
        final Path garbage = Files.writeString(tempDir.resolve("not-a-cert.crt"), "this is not a certificate");

        assertThrows(IllegalArgumentException.class, () -> MutualTlsArmeriaHttpAuthenticationProvider
                .buildTrustManagerFactory(garbage.toString()));
    }

    @Test
    void buildTrustManagerFactory_loads_der_encoded_certificate() throws Exception {
        final KeyPair derCaKeyPair = TestCertificateGenerator.generateKeyPair();
        final X509Certificate derCa = TestCertificateGenerator.generateCaCertificate(derCaKeyPair);
        final Path derFile = tempDir.resolve("ca.der");
        Files.write(derFile, derCa.getEncoded());

        final TrustManagerFactory trustManagerFactory = MutualTlsArmeriaHttpAuthenticationProvider
                .buildTrustManagerFactory(derFile.toString());

        assertThat(((X509TrustManager) trustManagerFactory.getTrustManagers()[0]).getAcceptedIssuers().length, is(1));
    }

    @Test
    void getClientAuthConfiguration_returns_non_empty() {
        MutualTlsAuthenticationConfig config = mock(MutualTlsAuthenticationConfig.class);
        when(config.getSslTrustCertificateFile()).thenReturn(caCertFile.toString());

        MutualTlsArmeriaHttpAuthenticationProvider provider = new MutualTlsArmeriaHttpAuthenticationProvider(config);

        assertThat(provider.getClientAuthConfiguration().isPresent(), is(true));
    }

    @Test
    void getClientAuthConfiguration_returns_client_auth_optional() {
        MutualTlsAuthenticationConfig config = mock(MutualTlsAuthenticationConfig.class);
        when(config.getSslTrustCertificateFile()).thenReturn(caCertFile.toString());

        MutualTlsArmeriaHttpAuthenticationProvider provider = new MutualTlsArmeriaHttpAuthenticationProvider(config);
        Optional<ClientAuthConfiguration> clientAuthConfig = provider.getClientAuthConfiguration();

        assertThat(clientAuthConfig.get().getClientAuth(), equalTo(ClientAuth.OPTIONAL));
        assertThat(clientAuthConfig.get().getTrustManagerFactory(), is(notNullValue()));
    }

    @Test
    void getAuthenticationDecorator_returns_decorator() {
        MutualTlsAuthenticationConfig config = mock(MutualTlsAuthenticationConfig.class);
        when(config.getSslTrustCertificateFile()).thenReturn(caCertFile.toString());

        MutualTlsArmeriaHttpAuthenticationProvider provider = new MutualTlsArmeriaHttpAuthenticationProvider(config);

        assertThat(provider.getAuthenticationDecorator().isPresent(), is(true));
    }

    @Test
    void hasVerifiedClientCertificate_returns_false_when_no_ssl_session() {
        ServiceRequestContext context = mock(ServiceRequestContext.class);
        when(context.sslSession()).thenReturn(null);

        assertThat(MutualTlsArmeriaHttpAuthenticationProvider
                .hasVerifiedClientCertificate(context, mock(HttpRequest.class))
                .toCompletableFuture().join(), is(false));
    }

    @Test
    void hasVerifiedClientCertificate_returns_false_when_peer_is_unverified() throws Exception {
        ServiceRequestContext context = mock(ServiceRequestContext.class);
        SSLSession sslSession = mock(SSLSession.class);
        when(context.sslSession()).thenReturn(sslSession);
        when(sslSession.getPeerCertificates()).thenThrow(new SSLPeerUnverifiedException("no client cert"));

        assertThat(MutualTlsArmeriaHttpAuthenticationProvider
                .hasVerifiedClientCertificate(context, mock(HttpRequest.class))
                .toCompletableFuture().join(), is(false));
    }

    @Test
    void hasVerifiedClientCertificate_returns_false_when_peer_certificates_empty() throws Exception {
        ServiceRequestContext context = mock(ServiceRequestContext.class);
        SSLSession sslSession = mock(SSLSession.class);
        when(context.sslSession()).thenReturn(sslSession);
        when(sslSession.getPeerCertificates()).thenReturn(new Certificate[0]);

        assertThat(MutualTlsArmeriaHttpAuthenticationProvider
                .hasVerifiedClientCertificate(context, mock(HttpRequest.class))
                .toCompletableFuture().join(), is(false));
    }

    @Test
    void hasVerifiedClientCertificate_returns_true_when_peer_certificate_present() throws Exception {
        ServiceRequestContext context = mock(ServiceRequestContext.class);
        SSLSession sslSession = mock(SSLSession.class);
        when(context.sslSession()).thenReturn(sslSession);
        when(sslSession.getPeerCertificates()).thenReturn(new Certificate[]{mock(X509Certificate.class)});

        assertThat(MutualTlsArmeriaHttpAuthenticationProvider
                .hasVerifiedClientCertificate(context, mock(HttpRequest.class))
                .toCompletableFuture().join(), is(true));
    }
}
