package org.opensearch.dataprepper.plugins.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.linecorp.armeria.server.Server;
import io.grpc.BindableService;
import io.grpc.ServerInterceptor;
import io.grpc.ServerServiceDefinition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.http.certificate.CertificateProviderFactory;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class CreateServerTest {
    ObjectMapper objectMapper;
    private final String TEST_SSL_CERTIFICATE_FILE = getClass().getClassLoader().getResource("test_cert.crt").getFile();
    private final String TEST_SSL_KEY_FILE = getClass().getClassLoader().getResource("test_decrypted_key.key").getFile();

    private static final RetryInfoConfig TEST_RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(50), Duration.ofMillis(2000));
    private String TEST_PIPELINE_NAME = "test-pipeline";
    private String TEST_SOURCE_NAME = "test-source";

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private CertificateProvider certificateProvider;

    private Logger LOG = LoggerFactory.getLogger(CreateServer.class);

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
    private ServerServiceDefinition serviceDef;

    @Mock
    private BindableService basicService;


    @Mock
    private Certificate certificate;

    private BlockingBuffer<Record<Log>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 1);
        integerHashMap.put("batch_size", 1);
        final PluginSetting pluginSetting = new PluginSetting("blocking_buffer", integerHashMap);
        pluginSetting.setPipelineName(TEST_PIPELINE_NAME);
        return new BlockingBuffer<>(pluginSetting);
    }

    //Me when idk how to mock a grpc service.  Tested in otel logs, metrics and trace i guess :(
//    @Test
//    void createGrpcServerTest() throws JsonProcessingException {
//        when(authenticationProvider.getAuthenticationInterceptor()).thenReturn(authenticationInterceptor);
//        MockedStatic<ServerInterceptors> mockedStatic = mockStatic(ServerInterceptors.class);
//        mockedStatic.when(() -> ServerInterceptors.intercept(
//                        any(ServerServiceDefinition.class),
//                        any(ServerInterceptor[].class)))
//                .thenReturn(serviceDef);
//        final Map<String, Object> metadata = createGrpcMetadata(21890, false, 10000, 10, 5, CompressionOption.NONE, null);
//        final ServerConfiguration serverConfiguration = createServerConfig(metadata);
//        final CreateServer createServer = new CreateServer(serverConfiguration, LOG, pluginMetrics, TEST_SOURCE_NAME, TEST_PIPELINE_NAME);
//        createServer.createGRPCServer(authenticationProvider, basicService, certificateProvider, null);
//    }

    @Test
    void createHttpServerTest() throws IOException {
        final Path certFilePath = new File(TEST_SSL_CERTIFICATE_FILE).toPath();
        final Path keyFilePath = new File(TEST_SSL_KEY_FILE).toPath();
        final String certAsString = Files.readString(certFilePath);
        final String keyAsString = Files.readString(keyFilePath);

        when(certificate.getCertificate()).thenReturn(certAsString);
        when(certificate.getPrivateKey()).thenReturn(keyAsString);
        when(certificateProvider.getCertificate()).thenReturn(certificate);
        when(certificateProviderFactory.getCertificateProvider()).thenReturn(certificateProvider);
        final Map<String, Object> metadata = createHttpMetadata(2021, "/log/ingest", 10_000, 200, 500, 1024, true, CompressionOption.NONE);
        final ServerConfiguration serverConfiguration = createServerConfig(metadata);
        final CreateServer createServer = new CreateServer(serverConfiguration, LOG, pluginMetrics, TEST_SOURCE_NAME, TEST_PIPELINE_NAME);
        Buffer<Record<Log>> buffer = getBuffer();
        Server server = createServer.createHTTPServer(buffer, certificateProviderFactory, armeriaAuthenticationProvider, httpRequestExceptionHandler);
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
        metadata.put("requestTimeoutInMillis", requestTimeoutInMillis);
        metadata.put("threadCount", threadCount);
        metadata.put("maxConnectionCount", maxConnectionCount);
        metadata.put("maxPendingRequests", maxPendingRequests);
        metadata.put("healthCheck", hasHealthCheckService);
        metadata.put("compression", compressionOption);
        return metadata;
    }

    private ServerConfiguration createServerConfig(final Map<String, Object> metadata) throws JsonProcessingException {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        String json = new ObjectMapper().writeValueAsString(metadata);
        return objectMapper.readValue(json, ServerConfiguration.class);
    }
}
