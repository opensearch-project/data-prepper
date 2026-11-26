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
import com.linecorp.armeria.server.Server;
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
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.MutualTlsArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.plugins.TestCertificateGenerator;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.cert.X509Certificate;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test of the mutual TLS wiring in {@link BaseHttpSource}: the {@code tlsCustomizer} that applies
 * the plugin's client-auth mode and trust manager, the 401 authentication decorator, and the runtime
 * guard that rejects {@code mutual_tls} without SSL.
 * <p>
 * The source configuration and {@code PluginFactory} are mocked and a minimal {@link BaseHttpSource}
 * subclass is used, so this is not an end-to-end test. End-to-end coverage with the real source
 * configuration, bean validation and real sources lives in {@code HTTPSourceMutualTlsIT} (http-source)
 * and {@code OpenSearchAPISourceMutualTlsIT} (opensearch-api-source).
 */
@ExtendWith(MockitoExtension.class)
class BaseHttpSourceMutualTlsTest {

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
        KeyPair caKeyPair = TestCertificateGenerator.generateKeyPair();
        X509Certificate caCert = TestCertificateGenerator.generateCaCertificate(caKeyPair);
        caCertFile = TestCertificateGenerator.writeCertificateToPem(caCert, tempDir.resolve("ca.crt"));

        KeyPair serverKeyPair = TestCertificateGenerator.generateKeyPair();
        X509Certificate serverCert = TestCertificateGenerator.generateSignedCertificate(
                serverKeyPair, caKeyPair, caCert, "localhost");
        serverCertFile = TestCertificateGenerator.writeCertificateToPem(serverCert, tempDir.resolve("server.crt"));
        serverKeyFile = TestCertificateGenerator.writePrivateKeyToPem(serverKeyPair, tempDir.resolve("server.key"));

        KeyPair clientKeyPair = TestCertificateGenerator.generateKeyPair();
        X509Certificate clientCert = TestCertificateGenerator.generateSignedCertificate(
                clientKeyPair, caKeyPair, caCert, "test-client");
        clientCertFile = TestCertificateGenerator.writeCertificateToPem(clientCert, tempDir.resolve("client.crt"));
        clientKeyFile = TestCertificateGenerator.writePrivateKeyToPem(clientKeyPair, tempDir.resolve("client.key"));
    }

    @BeforeEach
    void setUp() {
        String pipelineName = UUID.randomUUID().toString();
        lenient().when(pipelineDescription.getPipelineName()).thenReturn(pipelineName);

        lenient().when(sourceConfig.getPort()).thenReturn(0);
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
        lenient().when(sourceConfig.getAuthentication())
                .thenReturn(new PluginModel("mutual_tls", Map.of("ssl_trust_certificate_file", caCertFile.toString())));

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
    void client_with_valid_cert_connects_successfully() throws Exception {
        source.start(buffer);
        actualPort = getActivePort(source);

        ClientFactory factory = ClientFactory.builder()
                .tlsNoVerify()
                .tls(new File(clientCertFile.toString()), new File(clientKeyFile.toString()))
                .build();

        AggregatedHttpResponse response = WebClient.builder()
                .factory(factory)
                .build()
                .execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTPS)
                        .authority("127.0.0.1:" + actualPort)
                        .method(HttpMethod.GET)
                        .path("/health")
                        .build())
                .aggregate()
                .join();

        assertThat(response.status(), equalTo(HttpStatus.OK));
        factory.close();
    }

    @Test
    void client_without_cert_receives_401() throws Exception {
        source.start(buffer);
        actualPort = getActivePort(source);

        ClientFactory factory = ClientFactory.builder()
                .tlsNoVerify()
                .build();

        AggregatedHttpResponse response = WebClient.builder()
                .factory(factory)
                .build()
                .execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTPS)
                        .authority("127.0.0.1:" + actualPort)
                        .method(HttpMethod.GET)
                        .path("/health")
                        .build())
                .aggregate()
                .join();

        assertThat(response.status(), equalTo(HttpStatus.UNAUTHORIZED));
        factory.close();
    }

    @Test
    void mutual_tls_without_ssl_throws_on_start() {
        when(sourceConfig.isSsl()).thenReturn(false);

        assertThrows(IllegalStateException.class, () -> source.start(buffer));
    }

    private static int getActivePort(BaseHttpSource<?> source) throws Exception {
        Field serverField = BaseHttpSource.class.getDeclaredField("server");
        serverField.setAccessible(true);
        Server server = (Server) serverField.get(source);
        return server.activeLocalPort();
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
