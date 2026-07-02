/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.http;

import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.MutualTlsAuthenticationConfig;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.MutualTlsArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

import java.io.File;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MutualTlsIntegrationTest {

    private static final int PORT = 14443;

    @TempDir
    static Path tempDir;

    private static Path caCertFile;
    private static Path serverCertFile;
    private static Path serverKeyFile;
    private static Path clientCertFile;
    private static Path clientKeyFile;

    @Mock
    private HttpServerConfig sourceConfig;

    @Mock
    private PipelineDescription pipelineDescription;

    @Mock
    private PluginFactory pluginFactory;

    private BaseHttpSource<Record<Event>> source;
    private Buffer<Record<Event>> buffer;
    private int actualPort;

    @BeforeAll
    static void generateCerts() throws Exception {
        KeyPair caKeyPair = TestCertificateHelper.generateKeyPair();
        X509Certificate caCert = TestCertificateHelper.generateCaCertificate(caKeyPair);
        caCertFile = TestCertificateHelper.writeCertificateToPem(caCert, tempDir.resolve("ca.crt"));

        KeyPair serverKeyPair = TestCertificateHelper.generateKeyPair();
        X509Certificate serverCert = TestCertificateHelper.generateSignedCertificate(
                serverKeyPair, caKeyPair, caCert, "localhost");
        serverCertFile = TestCertificateHelper.writeCertificateToPem(serverCert, tempDir.resolve("server.crt"));
        serverKeyFile = TestCertificateHelper.writePrivateKeyToPem(serverKeyPair, tempDir.resolve("server.key"));

        KeyPair clientKeyPair = TestCertificateHelper.generateKeyPair();
        X509Certificate clientCert = TestCertificateHelper.generateSignedCertificate(
                clientKeyPair, caKeyPair, caCert, "test-client");
        clientCertFile = TestCertificateHelper.writeCertificateToPem(clientCert, tempDir.resolve("client.crt"));
        clientKeyFile = TestCertificateHelper.writePrivateKeyToPem(clientKeyPair, tempDir.resolve("client.key"));
    }

    @BeforeEach
    void setUp() {
        String pipelineName = UUID.randomUUID().toString();
        lenient().when(pipelineDescription.getPipelineName()).thenReturn(pipelineName);

        lenient().when(sourceConfig.getPort()).thenReturn(PORT);
        lenient().when(sourceConfig.getPath()).thenReturn("/");
        lenient().when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(10000);
        lenient().when(sourceConfig.getBufferTimeoutInMillis()).thenReturn(8000);
        lenient().when(sourceConfig.getThreadCount()).thenReturn(2);
        lenient().when(sourceConfig.getMaxConnectionCount()).thenReturn(100);
        lenient().when(sourceConfig.getMaxPendingRequests()).thenReturn(100);
        lenient().when(sourceConfig.getCompression()).thenReturn(CompressionOption.NONE);
        lenient().when(sourceConfig.hasHealthCheckService()).thenReturn(true);
        lenient().when(sourceConfig.isSsl()).thenReturn(true);
        lenient().when(sourceConfig.getSslCertificateFile()).thenReturn(serverCertFile.toString());
        lenient().when(sourceConfig.getSslKeyFile()).thenReturn(serverKeyFile.toString());

        MutualTlsAuthenticationConfig mtlsConfig = mock(MutualTlsAuthenticationConfig.class);
        when(mtlsConfig.getSslTrustCertificateFile()).thenReturn(caCertFile.toString());
        MutualTlsArmeriaHttpAuthenticationProvider mtlsProvider =
                new MutualTlsArmeriaHttpAuthenticationProvider(mtlsConfig);

        when(pluginFactory.loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(mtlsProvider);

        PluginMetrics pluginMetrics = PluginMetrics.fromNames("test_source", pipelineName);

        source = new TestHttpSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        buffer = new BlockingBuffer<>(10, 8, "test-pipeline");
    }

    @AfterEach
    void tearDown() {
        if (source != null) {
            source.stop();
        }
    }

    @Test
    void client_with_valid_cert_connects_successfully() {
        source.start(buffer);

        ClientFactory factory = ClientFactory.builder()
                .tlsNoVerify()
                .tls(new File(clientCertFile.toString()), new File(clientKeyFile.toString()))
                .build();

        AggregatedHttpResponse response = WebClient.builder()
                .factory(factory)
                .build()
                .execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTPS)
                        .authority("127.0.0.1:" + PORT)
                        .method(HttpMethod.GET)
                        .path("/health")
                        .build())
                .aggregate()
                .join();

        assertThat(response.status(), equalTo(HttpStatus.OK));
        factory.close();
    }

    @Test
    void client_without_cert_fails_tls_handshake() {
        source.start(buffer);

        ClientFactory factory = ClientFactory.builder()
                .tlsNoVerify()
                .build();

        assertThrows(Exception.class, () -> {
            WebClient.builder()
                    .factory(factory)
                    .build()
                    .execute(RequestHeaders.builder()
                            .scheme(SessionProtocol.HTTPS)
                            .authority("127.0.0.1:" + PORT)
                            .method(HttpMethod.GET)
                            .path("/health")
                            .build())
                    .aggregate()
                    .join();
        });

        factory.close();
    }

    @Test
    void mutual_tls_without_ssl_throws_on_start() {
        when(sourceConfig.isSsl()).thenReturn(false);

        assertThrows(IllegalStateException.class, () -> source.start(buffer));
    }

    private static class TestHttpSource extends BaseHttpSource<Record<Event>> {
        TestHttpSource(HttpServerConfig config, PluginMetrics metrics, PluginFactory factory, PipelineDescription desc) {
            super(config, metrics, factory, desc, "test-source",
                    org.slf4j.LoggerFactory.getLogger(TestHttpSource.class));
        }

        @Override
        public BaseHttpService getHttpService(int bufferTimeoutInMillis, Buffer<Record<Event>> buffer, PluginMetrics pluginMetrics) {
            return new TestService();
        }
    }

    @com.linecorp.armeria.server.annotation.Blocking
    private static class TestService implements BaseHttpService {
        @com.linecorp.armeria.server.annotation.Post("/test")
        public com.linecorp.armeria.common.HttpResponse doPost() {
            return com.linecorp.armeria.common.HttpResponse.of(HttpStatus.OK);
        }
    }
}
