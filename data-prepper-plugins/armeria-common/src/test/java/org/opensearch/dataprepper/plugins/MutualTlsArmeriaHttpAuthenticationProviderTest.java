/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins;

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.netty.handler.ssl.ClientAuth;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.ClientAuthConfiguration;
import org.opensearch.dataprepper.armeria.authentication.MutualTlsAuthenticationConfig;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;

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
    private static Path serverCertFile;
    private static Path serverKeyFile;
    private static Path clientCertFile;
    private static Path clientKeyFile;

    @BeforeAll
    static void generateCerts() throws Exception {
        KeyPair caKeyPair = TestCertificateGenerator.generateKeyPair();
        X509Certificate caCert = TestCertificateGenerator.generateCaCertificate(caKeyPair);
        caCertFile = TestCertificateGenerator.writeCertificateToPem(caCert, tempDir.resolve("ca.crt"));

        final KeyPair serverKeyPair = TestCertificateGenerator.generateKeyPair();
        final X509Certificate serverCert = TestCertificateGenerator.generateSignedCertificate(
                serverKeyPair, caKeyPair, caCert, "localhost");
        serverCertFile = TestCertificateGenerator.writeCertificateToPem(serverCert, tempDir.resolve("server.crt"));
        serverKeyFile = TestCertificateGenerator.writePrivateKeyToPem(serverKeyPair, tempDir.resolve("server.key"));

        final KeyPair clientKeyPair = TestCertificateGenerator.generateKeyPair();
        final X509Certificate clientCert = TestCertificateGenerator.generateSignedCertificate(
                clientKeyPair, caKeyPair, caCert, "trusted-client");
        clientCertFile = TestCertificateGenerator.writeCertificateToPem(clientCert, tempDir.resolve("client.crt"));
        clientKeyFile = TestCertificateGenerator.writePrivateKeyToPem(clientKeyPair, tempDir.resolve("client.key"));
    }

    @Test
    void plugin_annotation_declares_mutual_tls_contract() {
        final DataPrepperPlugin annotation =
                MutualTlsArmeriaHttpAuthenticationProvider.class.getAnnotation(DataPrepperPlugin.class);

        assertThat(annotation, is(notNullValue()));
        assertThat(annotation.name(), equalTo("mutual_tls"));
        assertThat(annotation.pluginType(), equalTo(ArmeriaHttpAuthenticationProvider.class));
        assertThat(annotation.pluginConfigurationType(), equalTo(MutualTlsAuthenticationConfig.class));
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

    /**
     * A server that applies only the authentication decorator, without the TLS client-auth wiring
     * from getClientAuthConfiguration(), never requests a client certificate. The decorator must
     * then reject every request, even from a client holding a certificate signed by the trusted CA.
     */
    @Test
    void decorator_without_client_auth_wiring_returns_401_for_trusted_client_certificate() {
        final MutualTlsAuthenticationConfig config = mock(MutualTlsAuthenticationConfig.class);
        when(config.getSslTrustCertificateFile()).thenReturn(caCertFile.toString());
        final MutualTlsArmeriaHttpAuthenticationProvider provider = new MutualTlsArmeriaHttpAuthenticationProvider(config);

        final Server server = Server.builder()
                .https(0)
                .tls(serverCertFile.toFile(), serverKeyFile.toFile())
                .service("/", (ctx, req) -> HttpResponse.of(HttpStatus.OK))
                .decorator(provider.getAuthenticationDecorator().orElseThrow())
                .build();
        server.start().join();

        try (ClientFactory clientFactory = ClientFactory.builder()
                .tlsNoVerify()
                .tls(clientCertFile.toFile(), clientKeyFile.toFile())
                .build()) {
            final AggregatedHttpResponse response = WebClient.builder("https://127.0.0.1:" + server.activeLocalPort())
                    .factory(clientFactory)
                    .build()
                    .get("/")
                    .aggregate()
                    .join();

            assertThat(response.status(), equalTo(HttpStatus.UNAUTHORIZED));
        } finally {
            server.stop().join();
        }
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
