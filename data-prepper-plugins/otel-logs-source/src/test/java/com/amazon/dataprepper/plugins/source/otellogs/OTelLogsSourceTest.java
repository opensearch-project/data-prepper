/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.source.otellogs;


import com.amazon.dataprepper.plugins.health.HealthGrpcService;
import com.amazon.dataprepper.plugins.source.otellogs.certificate.CertificateProviderFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.netty.util.AsciiString;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.GrpcBasicAuthenticationProvider;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static com.amazon.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.SSL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class OTelLogsSourceTest {

    @Mock
    private ServerBuilder serverBuilder;

    @Mock
    private Server server;

    @Mock
    private GrpcServiceBuilder grpcServiceBuilder;

    @Mock
    private GrpcService grpcService;

    @Mock
    private CertificateProviderFactory certificateProviderFactory;

    @Mock
    private CertificateProvider certificateProvider;

    @Mock
    private Certificate certificate;

    @Mock
    private CompletableFuture<Void> completableFuture;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private GrpcBasicAuthenticationProvider authenticationProvider;

    private BlockingBuffer<Record<Object>> buffer;
    private PluginSetting pluginSetting;
    private PluginSetting testPluginSetting;
    private OTelLogsSourceConfig oTelLogsSourceConfig;
    private PluginMetrics pluginMetrics;
    private PipelineDescription pipelineDescription;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();



    private OTelLogsSource SOURCE;

    private static final String TEST_PIPELINE_NAME = "test_pipeline";

    private static final ExportLogsServiceRequest LOGS_REQUEST = ExportLogsServiceRequest.newBuilder()
            .addResourceLogs(ResourceLogs.newBuilder().build()).build();

    private static void assertStatusCode415AndNoServerHeaders(final AggregatedHttpResponse response, final Throwable throwable) {
        assertThat("Http Status", response.status(), is(HttpStatus.UNSUPPORTED_MEDIA_TYPE));
        assertThat("Http Response Throwable", throwable, is(nullValue()));

        final List<String> headerKeys = response.headers()
                .stream()
                .map(Map.Entry::getKey)
                .map(AsciiString::toString)
                .collect(Collectors.toList());
        assertThat("Response Header Keys", headerKeys, not(contains("server")));
    }

    private BlockingBuffer<Record<Object>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 1);
        integerHashMap.put("batch_size", 1);
        return new BlockingBuffer<>(new PluginSetting("blocking_buffer", integerHashMap));
    }

    @BeforeEach
    public void beforeEach() {
        lenient().when(serverBuilder.service(any(GrpcService.class))).thenReturn(serverBuilder);
        lenient().when(serverBuilder.http(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.https(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);
        lenient().when(server.start()).thenReturn(completableFuture);

        lenient().when(grpcServiceBuilder.addService(any(BindableService.class))).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useClientTimeoutHeader(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useBlockingTaskExecutor(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.build()).thenReturn(grpcService);

        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("request_timeout", 5);
        settingsMap.put(SSL, false);
        pluginSetting = new PluginSetting("otel_logs", settingsMap);
        pluginSetting.setPipelineName("pipeline");
        pluginMetrics = PluginMetrics.fromNames("otel_logs", "pipeline");

        oTelLogsSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelLogsSourceConfig.class);

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(authenticationProvider);
        pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        SOURCE = new OTelLogsSource(oTelLogsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        buffer = getBuffer();
    }

    @AfterEach
    public void afterEach() {
        SOURCE.stop();
    }

    @Test
    void testHttpFullJson() throws InvalidProtocolBufferException {
        SOURCE.start(buffer);
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21892")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.logs.v1.LogsService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(LOGS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete(OTelLogsSourceTest::assertStatusCode415AndNoServerHeaders)
                .join();
    }

    @Test
    void testHttpsFullJson() throws InvalidProtocolBufferException {

        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("request_timeout", 5);
        settingsMap.put(SSL, true);
        settingsMap.put("useAcmCertForSSL", false);
        settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
        settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");
        pluginSetting = new PluginSetting("otel_logs", settingsMap);
        pluginSetting.setPipelineName("pipeline");

        oTelLogsSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelLogsSourceConfig.class);
        SOURCE = new OTelLogsSource(oTelLogsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        buffer = getBuffer();
        SOURCE.start(buffer);

        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTPS)
                        .authority("127.0.0.1:21892")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.logs.v1.LogsService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(LOGS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete(OTelLogsSourceTest::assertStatusCode415AndNoServerHeaders)
                .join();
    }

    @Test
    void testHttpFullBytes() {
        SOURCE.start(buffer);
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21892")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.logs.v1.LogsService/Export")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.copyOf(LOGS_REQUEST.toByteArray()))
                .aggregate()
                .whenComplete(OTelLogsSourceTest::assertStatusCode415AndNoServerHeaders)
                .join();
    }

    @Test
    public void testServerStartCertFileSuccess() throws IOException {
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(server.stop()).thenReturn(completableFuture);

            final Path certFilePath = Path.of("data/certificate/test_cert.crt");
            final Path keyFilePath = Path.of("data/certificate/test_decrypted_key.key");
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);

            final Map<String, Object> settingsMap = new HashMap<>();
            settingsMap.put(SSL, true);
            settingsMap.put("useAcmCertForSSL", false);
            settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
            settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");

            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");
            oTelLogsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelLogsSourceConfig.class);
            final OTelLogsSource source = new OTelLogsSource(oTelLogsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
            source.start(buffer);
            source.stop();

            final ArgumentCaptor<InputStream> certificateIs = ArgumentCaptor.forClass(InputStream.class);
            final ArgumentCaptor<InputStream> privateKeyIs = ArgumentCaptor.forClass(InputStream.class);
            verify(serverBuilder).tls(certificateIs.capture(), privateKeyIs.capture());
            final String actualCertificate = IOUtils.toString(certificateIs.getValue(), StandardCharsets.UTF_8.name());
            final String actualPrivateKey = IOUtils.toString(privateKeyIs.getValue(), StandardCharsets.UTF_8.name());
            assertThat(actualCertificate, is(certAsString));
            assertThat(actualPrivateKey, is(keyAsString));
        }
    }

    @Test
    public void testServerStartACMCertSuccess() throws IOException {
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(server.stop()).thenReturn(completableFuture);
            final Path certFilePath = Path.of("data/certificate/test_cert.crt");
            final Path keyFilePath = Path.of("data/certificate/test_decrypted_key.key");
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);
            when(certificate.getCertificate()).thenReturn(certAsString);
            when(certificate.getPrivateKey()).thenReturn(keyAsString);
            when(certificateProvider.getCertificate()).thenReturn(certificate);
            when(certificateProviderFactory.getCertificateProvider()).thenReturn(certificateProvider);
            final Map<String, Object> settingsMap = new HashMap<>();
            settingsMap.put(SSL, true);
            settingsMap.put("useAcmCertForSSL", true);
            settingsMap.put("awsRegion", "us-east-1");
            settingsMap.put("acmCertificateArn", "arn:aws:acm:us-east-1:account:certificate/1234-567-856456");
            settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
            settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");

            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");
            oTelLogsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelLogsSourceConfig.class);
            final OTelLogsSource source = new OTelLogsSource(oTelLogsSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
            source.start(buffer);
            source.stop();

            final ArgumentCaptor<InputStream> certificateIs = ArgumentCaptor.forClass(InputStream.class);
            final ArgumentCaptor<InputStream> privateKeyIs = ArgumentCaptor.forClass(InputStream.class);
            verify(serverBuilder).tls(certificateIs.capture(), privateKeyIs.capture());
            final String actualCertificate = IOUtils.toString(certificateIs.getValue(), StandardCharsets.UTF_8.name());
            final String actualPrivateKey = IOUtils.toString(privateKeyIs.getValue(), StandardCharsets.UTF_8.name());
            assertThat(actualCertificate, is(certAsString));
            assertThat(actualPrivateKey, is(keyAsString));
        }
    }

    @Test
    void start_with_Health_configured_includes_HealthCheck_service() throws IOException {
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class);
            MockedStatic<GrpcService> grpcServerMock = Mockito.mockStatic(GrpcService.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            grpcServerMock.when(GrpcService::builder).thenReturn(grpcServiceBuilder);
            when(grpcServiceBuilder.addService(any(ServerServiceDefinition.class))).thenReturn(grpcServiceBuilder);
            when(grpcServiceBuilder.useClientTimeoutHeader(anyBoolean())).thenReturn(grpcServiceBuilder);

            when(server.stop()).thenReturn(completableFuture);
            final Path certFilePath = Path.of("data/certificate/test_cert.crt");
            final Path keyFilePath = Path.of("data/certificate/test_decrypted_key.key");
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);
            when(certificate.getCertificate()).thenReturn(certAsString);
            when(certificate.getPrivateKey()).thenReturn(keyAsString);
            when(certificateProvider.getCertificate()).thenReturn(certificate);
            when(certificateProviderFactory.getCertificateProvider()).thenReturn(certificateProvider);
            final Map<String, Object> settingsMap = new HashMap<>();
            settingsMap.put(SSL, true);
            settingsMap.put("useAcmCertForSSL", true);
            settingsMap.put("awsRegion", "us-east-1");
            settingsMap.put("acmCertificateArn", "arn:aws:acm:us-east-1:account:certificate/1234-567-856456");
            settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
            settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");
            settingsMap.put("health_check_service", "true");

            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");

            oTelLogsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelLogsSourceConfig.class);
            final OTelLogsSource source = new OTelLogsSource(oTelLogsSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
            source.start(buffer);
            source.stop();
        }

        verify(grpcServiceBuilder, times(1)).useClientTimeoutHeader(false);
        verify(grpcServiceBuilder, times(1)).useBlockingTaskExecutor(true);
        verify(grpcServiceBuilder).addService(isA(HealthGrpcService.class));
    }

    @Test
    void start_without_Health_configured_does_not_include_HealthCheck_service() throws IOException {
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class);
            MockedStatic<GrpcService> grpcServerMock = Mockito.mockStatic(GrpcService.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            grpcServerMock.when(GrpcService::builder).thenReturn(grpcServiceBuilder);
            when(grpcServiceBuilder.addService(any(ServerServiceDefinition.class))).thenReturn(grpcServiceBuilder);
            when(grpcServiceBuilder.useClientTimeoutHeader(anyBoolean())).thenReturn(grpcServiceBuilder);

            when(server.stop()).thenReturn(completableFuture);
            final Path certFilePath = Path.of("data/certificate/test_cert.crt");
            final Path keyFilePath = Path.of("data/certificate/test_decrypted_key.key");
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);
            when(certificate.getCertificate()).thenReturn(certAsString);
            when(certificate.getPrivateKey()).thenReturn(keyAsString);
            when(certificateProvider.getCertificate()).thenReturn(certificate);
            when(certificateProviderFactory.getCertificateProvider()).thenReturn(certificateProvider);
            final Map<String, Object> settingsMap = new HashMap<>();
            settingsMap.put(SSL, true);
            settingsMap.put("useAcmCertForSSL", true);
            settingsMap.put("awsRegion", "us-east-1");
            settingsMap.put("acmCertificateArn", "arn:aws:acm:us-east-1:account:certificate/1234-567-856456");
            settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
            settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");
            settingsMap.put("health_check_service", "false");

            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");
            oTelLogsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelLogsSourceConfig.class);
            final OTelLogsSource source = new OTelLogsSource(oTelLogsSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
            source.start(buffer);
            source.stop();
        }

        verify(grpcServiceBuilder, times(1)).useClientTimeoutHeader(false);
        verify(grpcServiceBuilder, times(1)).useBlockingTaskExecutor(true);
        verify(grpcServiceBuilder, never()).addService(isA(HealthGrpcService.class));
    }

    @Test
    public void testDoubleStart() {
        // starting server
        SOURCE.start(buffer);
        // double start server
        Assertions.assertThrows(IllegalStateException.class, () -> SOURCE.start(buffer));
    }

    @Test
    public void testRunAnotherSourceWithSamePort() {
        // starting server
        SOURCE.start(buffer);

        testPluginSetting = new PluginSetting(null, Collections.singletonMap(SSL, false));
        testPluginSetting.setPipelineName("pipeline");
        oTelLogsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelLogsSourceConfig.class);
        final OTelLogsSource source = new OTelLogsSource(oTelLogsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        //Expect RuntimeException because when port is already in use, BindException is thrown which is not RuntimeException
        Assertions.assertThrows(RuntimeException.class, () -> source.start(buffer));
    }

    @Test
    public void testStartWithEmptyBuffer() {
        testPluginSetting = new PluginSetting(null, Collections.singletonMap(SSL, false));
        testPluginSetting.setPipelineName("pipeline");
        oTelLogsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelLogsSourceConfig.class);
        final OTelLogsSource source = new OTelLogsSource(oTelLogsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        Assertions.assertThrows(IllegalStateException.class, () -> source.start(null));
    }

    @Test
    public void testStartWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelLogsSource source = new OTelLogsSource(oTelLogsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));

            // When/Then
            assertThrows(RuntimeException.class, () -> source.start(buffer));
        }
    }

    @Test
    public void testStartWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelLogsSource source = new OTelLogsSource(oTelLogsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            final NullPointerException expCause = new NullPointerException();
            when(completableFuture.get()).thenThrow(new ExecutionException("", expCause));

            // When/Then
            final RuntimeException ex = assertThrows(RuntimeException.class, () -> source.start(buffer));
            assertEquals(expCause, ex);
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelLogsSource source = new OTelLogsSource(oTelLogsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(buffer);
            when(server.stop()).thenReturn(completableFuture);

            // When/Then
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));
            assertThrows(RuntimeException.class, source::stop);
        }
    }

    @Test
    public void testStartWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelLogsSource source = new OTelLogsSource(oTelLogsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            assertThrows(RuntimeException.class, () -> source.start(buffer));
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelLogsSource source = new OTelLogsSource(oTelLogsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(buffer);
            when(server.stop()).thenReturn(completableFuture);
            final NullPointerException expCause = new NullPointerException();
            when(completableFuture.get()).thenThrow(new ExecutionException("", expCause));

            // When/Then
            final RuntimeException ex = assertThrows(RuntimeException.class, source::stop);
            assertEquals(expCause, ex);
        }
    }

    @Test
    public void testStopWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelLogsSource source = new OTelLogsSource(oTelLogsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(buffer);
            when(server.stop()).thenReturn(completableFuture);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            assertThrows(RuntimeException.class, source::stop);
            assertTrue(Thread.interrupted());
        }
    }
}
