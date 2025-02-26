package org.opensearch.dataprepper.plugins.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpRequest;
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServiceRequestContext;
import io.grpc.ServerInterceptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.http.BaseHttpServerConfig;
import org.opensearch.dataprepper.http.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateServerTest {
    private Logger LOG = LoggerFactory.getLogger(CreateServer.class);
    ObjectMapper objectMapper;
    private final String TEST_SSL_CERTIFICATE_FILE = getClass().getClassLoader().getResource("test_cert.crt").getFile();
    private final String TEST_SSL_KEY_FILE = getClass().getClassLoader().getResource("test_decrypted_key.key").getFile();
    private String TEST_PIPELINE_NAME = "test-pipeline";
    private String TEST_SOURCE_NAME = "test-source";

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private CertificateProvider certificateProvider;

    @Mock
    private CertificateProviderFactory certificateProviderFactory;

    @Mock
    private ArmeriaHttpAuthenticationProvider armeriaAuthenticationProvider;

    @Mock
    private HttpRequestExceptionHandler httpRequestExceptionHandler;

    @Mock
    private GrpcAuthenticationProvider authenticationProvider;

    @Mock
    ServerInterceptor authenticationInterceptor;

    @Mock
    private Certificate certificate;

    @Test
    void createGrpcServerTest() throws JsonProcessingException {
        when(authenticationProvider.getAuthenticationInterceptor()).thenReturn(authenticationInterceptor);
        final Map<String, Object> metadata = createGrpcMetadata(21890, false, 10000, 10, 5, CompressionOption.NONE, null);
        final ServerConfiguration serverConfiguration = createServerConfig(metadata);
        final CreateServer createServer = new CreateServer(serverConfiguration, LOG, pluginMetrics, TEST_SOURCE_NAME, TEST_PIPELINE_NAME);
        Buffer<Record<? extends Metric>> buffer = new BlockingBuffer<Record<? extends Metric>>(TEST_PIPELINE_NAME);
        TestService testService = getTestService(buffer);

        Server server = createServer.createGRPCServer(authenticationProvider, testService, certificateProvider, null);

        assertNotNull(server);
        assertDoesNotThrow(() -> server.start());
        assertDoesNotThrow(() -> server.stop());
    }

    @Test
    void testCustomAuthModule() throws JsonProcessingException{
        when(authenticationProvider.getAuthenticationInterceptor()).thenReturn(authenticationInterceptor);
        final Map<String, Object> metadata = createGrpcMetadata(21890, false, 10000, 10, 5, CompressionOption.NONE, null);

        Map<String, Object> authConfig = new HashMap<>();
        authConfig.put("plugin", "test-auth");
        authConfig.put("settings", Collections.singletonMap("key", "value"));
        metadata.put("authentication", authConfig);
        final ServerConfiguration serverConfiguration = createServerConfig(metadata);

        SimpleAuthDecorator customAuth = new SimpleAuthDecorator();
        Logger mockLogger = mock(Logger.class);
        customAuth.setLogger(mockLogger);
        when(authenticationProvider.getHttpAuthenticationService()).thenReturn(Optional.of(customAuth));

        final CreateServer createServer = new CreateServer(serverConfiguration, LOG, pluginMetrics, TEST_SOURCE_NAME, TEST_PIPELINE_NAME);
        Buffer<Record<? extends Metric>> buffer = new BlockingBuffer<Record<? extends Metric>>(TEST_PIPELINE_NAME);
        TestService testService = getTestService(buffer);
        Server server = createServer.createGRPCServer(authenticationProvider, testService, certificateProvider, null);

        assertNotNull(server);
        assertDoesNotThrow(() -> server.start());

        WebClient webClient = WebClient.builder(
                String.format("http://127.0.0.1:%d", serverConfiguration.getPort())
        ).build();
        webClient.get("/").aggregate().join();

        verify(mockLogger).info("Ensure Custom Auth Decorator is working");

        assertDoesNotThrow(() -> server.stop());
    }

    @Test
    void createHttpServerTest() throws IOException {
        final Path certFilePath = new File(TEST_SSL_CERTIFICATE_FILE).toPath();
        final Path keyFilePath = new File(TEST_SSL_KEY_FILE).toPath();

        final Map<String, Object> metadata = createHttpMetadata(2021, "/log/ingest", 10_000, 200, 500, 1024, true, CompressionOption.NONE);
        final BaseHttpServerConfig serverConfiguration = createHttpServerConfig(metadata);
        final CreateServer createServer = new CreateServer(LOG, pluginMetrics, TEST_SOURCE_NAME, TEST_PIPELINE_NAME);
        Buffer<Record<Log>> buffer = new BlockingBuffer<Record<Log>>(TEST_PIPELINE_NAME);
        String logService = "placeholder";
        Server server = createServer.createHTTPServer(buffer, certificateProviderFactory, armeriaAuthenticationProvider, httpRequestExceptionHandler, logService, serverConfiguration);
        assertNotNull(server);
        assertDoesNotThrow(() -> server.start());
        assertDoesNotThrow(() -> server.stop());
    }



    private Map<String, Object> createGrpcMetadata (Integer port, Boolean ssl, Integer reqeustTimeoutInMillis, Integer maxConnectionCount, Integer threadCount, CompressionOption compression, RetryInfoConfig retryInfo){
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("port", port);
        metadata.put("ssl", ssl);
        metadata.put("requestTimeoutInMillis", reqeustTimeoutInMillis);
        metadata.put("maxConnectionCount", maxConnectionCount);
        metadata.put("threadCount", threadCount);
        metadata.put("compression", compression);
        metadata.put("retryInfo", retryInfo);
        return metadata;
    }

    private Map<String, Object> createHttpMetadata (Integer port, String path, Integer requestTimeoutInMillis, Integer threadCount, Integer maxConnectionCount, Integer maxPendingRequests, Boolean hasHealthCheckService, CompressionOption compressionOption){
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("port", port);
        metadata.put("path", path);
        metadata.put("request_timeout", requestTimeoutInMillis);
        metadata.put("thread_count", threadCount);
        metadata.put("max_connection_count", maxConnectionCount);
        metadata.put("max_pending_requests", maxPendingRequests);
        metadata.put("health_check_service", hasHealthCheckService);
        metadata.put("compression", compressionOption);
        return metadata;
    }

    private BaseHttpServerConfig createHttpServerConfig(final Map<String, Object> metadata) throws JsonProcessingException {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        String json = new ObjectMapper().writeValueAsString(metadata);
        return objectMapper.readValue(json, BaseHttpServerConfig.class);
    }

    private ServerConfiguration createServerConfig(final Map<String, Object> metadata) throws JsonProcessingException {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        String json = new ObjectMapper().writeValueAsString(metadata);
        return objectMapper.readValue(json, GRPCServer.class);
    }

    private static class GRPCServer extends ServerConfiguration {
    }

    private TestService getTestService(Buffer<Record<? extends Metric>> buffer){
        TestService testService = new TestService(
                80,
                new OTelProtoCodec.OTelProtoDecoder(),
                buffer,
                pluginMetrics
        );
        return testService;
    }

    public class SimpleAuthDecorator implements Function<HttpService, HttpService> {
        private Logger authLog = LoggerFactory.getLogger(SimpleAuthDecorator.class);

        void setLogger(Logger logger) {
            this.authLog = logger;
        }

        @Override
        public HttpService apply(HttpService delegate) {
            return new HttpService() {
                @Override
                public com.linecorp.armeria.common.HttpResponse serve(ServiceRequestContext ctx, HttpRequest request) throws Exception {
                    authLog.info("Ensure Custom Auth Decorator is working");
                    return delegate.serve(ctx, request);
                }

            };
        }
    }

}
