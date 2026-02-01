/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.otellogs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.netty.util.AsciiString;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.resource.v1.Resource;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.GrpcRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.GrpcBasicAuthenticationProvider;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.server.HealthGrpcService;
import org.opensearch.dataprepper.plugins.source.otellogs.certificate.CertificateProviderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.otellogs.OTelLogsSourceConfig.SSL;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigFixture.createBuilderForConfigWithAcmeSsl;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigFixture.createConfigBuilderWithBasicAuth;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigFixture.createDefaultConfig;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigFixture.createBuilderForConfigWithSsl;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.BASIC_AUTH_PASSWORD;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.BASIC_AUTH_USERNAME;

@ExtendWith(MockitoExtension.class)
class OTelLogsSourceGrpcTest {
    private static final String GRPC_ENDPOINT = "gproto+http://127.0.0.1:21892/";
    private static final String TEST_PIPELINE_NAME = "test_pipeline";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ExportLogsServiceRequest LOGS_REQUEST = ExportLogsServiceRequest.newBuilder()
            .addResourceLogs(ResourceLogs.newBuilder().build()).build();

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

    @Mock
    private ArmeriaHttpAuthenticationProvider httpAuthenticationProvider;

    @Mock
    private BlockingBuffer<Record<Object>> buffer;

    private PluginMetrics pluginMetrics;
    private PipelineDescription pipelineDescription;
    private OTelLogsSource SOURCE;

    @BeforeEach
    public void beforeEach() {
        lenient().when(serverBuilder.service(any(GrpcService.class))).thenReturn(serverBuilder);
        lenient().when(serverBuilder.http(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.https(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);
        lenient().when(serverBuilder.port(anyInt(), (SessionProtocol) any())).thenReturn(serverBuilder);
        lenient().when(server.start()).thenReturn(completableFuture);

        lenient().when(grpcServiceBuilder.addService(any(BindableService.class))).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useClientTimeoutHeader(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useBlockingTaskExecutor(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.build()).thenReturn(grpcService);

        MetricsTestUtil.initMetrics();
        pluginMetrics = PluginMetrics.fromNames("otel_logs", "pipeline");

        lenient().when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(authenticationProvider);
        lenient().when(pluginFactory.loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(httpAuthenticationProvider);
        pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);

        setupLogsSource(createDefaultConfig());
    }

    @AfterEach
    public void afterEach() {
        SOURCE.stop();
    }

    private void setupLogsSource(OTelLogsSourceConfig config) {
        SOURCE = new OTelLogsSource(config, pluginMetrics, pluginFactory, pipelineDescription);
    }

    @Test
    void testServerStartCertFileSuccess() throws IOException {
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

            final OTelLogsSource source = new OTelLogsSource(createBuilderForConfigWithSsl().build(), pluginMetrics, pluginFactory, pipelineDescription);
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
    void testServerStartACMCertSuccess() throws IOException {
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
            final OTelLogsSource source = new OTelLogsSource(createBuilderForConfigWithAcmeSsl().build(), pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
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
            when(grpcServiceBuilder.exceptionHandler(any(GrpcRequestExceptionHandler.class))).thenReturn(grpcServiceBuilder);

            when(server.stop()).thenReturn(completableFuture);
            final Path certFilePath = Path.of("data/certificate/test_cert.crt");
            final Path keyFilePath = Path.of("data/certificate/test_decrypted_key.key");
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);
            when(certificate.getCertificate()).thenReturn(certAsString);
            when(certificate.getPrivateKey()).thenReturn(keyAsString);
            when(certificateProvider.getCertificate()).thenReturn(certificate);
            when(certificateProviderFactory.getCertificateProvider()).thenReturn(certificateProvider);

            final OTelLogsSource source = new OTelLogsSource(createBuilderForConfigWithAcmeSsl().build(), pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
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
            when(grpcServiceBuilder.exceptionHandler(any(
                    GrpcRequestExceptionHandler.class))).thenReturn(grpcServiceBuilder);

            when(server.stop()).thenReturn(completableFuture);
            final Path certFilePath = Path.of("data/certificate/test_cert.crt");
            final Path keyFilePath = Path.of("data/certificate/test_decrypted_key.key");
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);
            when(certificate.getCertificate()).thenReturn(certAsString);
            when(certificate.getPrivateKey()).thenReturn(keyAsString);
            when(certificateProvider.getCertificate()).thenReturn(certificate);
            when(certificateProviderFactory.getCertificateProvider()).thenReturn(certificateProvider);

            final OTelLogsSource source = new OTelLogsSource(createBuilderForConfigWithAcmeSsl().healthCheck(false).build(), pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
            source.start(buffer);
            source.stop();
        }

        verify(grpcServiceBuilder, times(1)).useClientTimeoutHeader(false);
        verify(grpcServiceBuilder, times(1)).useBlockingTaskExecutor(true);
        verify(grpcServiceBuilder, never()).addService(isA(HealthGrpcService.class));
    }

    @Test
    void testDoubleStart() {
        SOURCE.start(buffer);
        assertThrows(IllegalStateException.class, () -> SOURCE.start(buffer));
    }

    @Test
    void testRunAnotherSourceWithSamePort() {
        SOURCE.start(buffer);

        final OTelLogsSource source = new OTelLogsSource(createDefaultConfig(), pluginMetrics, pluginFactory, pipelineDescription);

        //Expect RuntimeException because when port is already in use, BindException is thrown which is not RuntimeException
        assertThrows(RuntimeException.class, () -> source.start(buffer));
    }

    @Test
    void testStartWithEmptyBuffer() {
        final OTelLogsSource source = new OTelLogsSource(createDefaultConfig(), pluginMetrics, pluginFactory, pipelineDescription);
        assertThrows(IllegalStateException.class, () -> source.start(null));
    }

    @Test
    void testStartWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelLogsSource source = new OTelLogsSource(createDefaultConfig(), pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));

            // When/Then
            assertThrows(RuntimeException.class, () -> source.start(buffer));
        }
    }

    @Test
    void testStartWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelLogsSource source = new OTelLogsSource(createDefaultConfig(), pluginMetrics, pluginFactory, pipelineDescription);
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
    void testStopWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        final OTelLogsSource source = new OTelLogsSource(createDefaultConfig(), pluginMetrics, pluginFactory, pipelineDescription);
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
    void testStartWithInterruptedException() throws ExecutionException, InterruptedException {
        final OTelLogsSource source = new OTelLogsSource(createDefaultConfig(), pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            assertThrows(RuntimeException.class, () -> source.start(buffer));
            assertTrue(Thread.interrupted());
        }
    }

    @Test
    void testStopWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelLogsSource source = new OTelLogsSource(createDefaultConfig(), pluginMetrics, pluginFactory, pipelineDescription);
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
    void testStopWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelLogsSource source = new OTelLogsSource(createDefaultConfig(), pluginMetrics, pluginFactory, pipelineDescription);
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

    @Test
    void http_request_writes_to_buffer_with_successful_response() throws Exception {
        SOURCE.start(buffer);

        final LogsServiceGrpc.LogsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .build(LogsServiceGrpc.LogsServiceBlockingStub.class);

        final ExportLogsServiceResponse exportResponse = client.export(createExportLogsRequest());
        assertThat(exportResponse, notNullValue());

        final ArgumentCaptor<Collection<Record<Object>>> bufferWriteArgumentCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(buffer).writeAll(bufferWriteArgumentCaptor.capture(), anyInt());

        final Collection<Record<Object>> actualBufferWrites = bufferWriteArgumentCaptor.getValue();
        assertThat(actualBufferWrites, notNullValue());
        assertThat(actualBufferWrites, hasSize(1));
    }

    @Test
    void gRPC_with_auth_request_writes_to_buffer_with_successful_response() throws Exception {
        GrpcBasicAuthenticationProvider authProvider = new GrpcBasicAuthenticationProvider(new HttpBasicAuthenticationConfig(BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD));
        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(authProvider);
        setupLogsSource(createConfigBuilderWithBasicAuth().build());
        SOURCE.start(buffer);

        final String encodeToString = Base64.getEncoder()
                .encodeToString(String.format("%s:%s", BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD).getBytes(StandardCharsets.UTF_8));

        final LogsServiceGrpc.LogsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .addHeader("Authorization", "Basic " + encodeToString)
                .build(LogsServiceGrpc.LogsServiceBlockingStub.class);
        final ExportLogsServiceResponse exportResponse = client.export(createExportLogsRequest());
        assertThat(exportResponse, notNullValue());

        final ArgumentCaptor<Collection<Record<Object>>> bufferWriteArgumentCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(buffer).writeAll(bufferWriteArgumentCaptor.capture(), anyInt());

        final Collection<Record<Object>> actualBufferWrites = bufferWriteArgumentCaptor.getValue();
        assertThat(actualBufferWrites, notNullValue());
        assertThat(actualBufferWrites, hasSize(1));
    }

    @Test
    void gRPC_with_auth_request_with_different_basic_auth_credentials_does_not_write_to_buffer_with_401_response() throws Exception {
        GrpcBasicAuthenticationProvider authProvider = new GrpcBasicAuthenticationProvider(new HttpBasicAuthenticationConfig("username", "wrong password"));
        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(authProvider);
        setupLogsSource(createConfigBuilderWithBasicAuth().build());
        SOURCE.start(buffer);

        final String givenCredentials = Base64.getEncoder()
                .encodeToString(String.format("%s:%s", "wrong Username", "wrong Password").getBytes(StandardCharsets.UTF_8));

        final LogsServiceGrpc.LogsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .addHeader("Authorization", "Basic " + givenCredentials)
                .build(LogsServiceGrpc.LogsServiceBlockingStub.class);

        StatusRuntimeException actualException = assertThrows(
                StatusRuntimeException.class,
                () -> client.export(createExportLogsRequest())
        );

        assertEquals(Status.Code.UNAUTHENTICATED, actualException.getStatus().getCode());

        verify(buffer, never()).writeAll(any(), anyInt());
    }

    @ParameterizedTest
    @ArgumentsSource(BufferExceptionToStatusArgumentsProvider.class)
    void gRPC_request_returns_expected_status_for_exceptions_from_buffer(
            final Class<Exception> bufferExceptionClass,
            final Status.Code expectedStatusCode) throws Exception {
        SOURCE.start(buffer);

        final LogsServiceGrpc.LogsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .build(LogsServiceGrpc.LogsServiceBlockingStub.class);

        doThrow(bufferExceptionClass)
                .when(buffer)
                .writeAll(anyCollection(), anyInt());
        final ExportLogsServiceRequest exportLogsRequest = createExportLogsRequest();
        final StatusRuntimeException actualException = assertThrows(StatusRuntimeException.class, () -> client.export(exportLogsRequest));

        assertThat(actualException.getStatus(), notNullValue());
        assertThat(actualException.getStatus().getCode(), equalTo(expectedStatusCode));
    }

    static class BufferExceptionToStatusArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(TimeoutException.class, Status.Code.RESOURCE_EXHAUSTED),
                    arguments(SizeOverflowException.class, Status.Code.RESOURCE_EXHAUSTED),
                    arguments(Exception.class, Status.Code.INTERNAL),
                    arguments(RuntimeException.class, Status.Code.INTERNAL)
            );
        }
    }

    private ExportLogsServiceRequest createExportLogsRequest() {
        final Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue("service").build())
                ).build();

        final ResourceLogs resourceLogs = ResourceLogs.newBuilder()
                .addScopeLogs(ScopeLogs.newBuilder()
                        .addLogRecords(LogRecord.newBuilder().setSeverityNumberValue(1))
                        .build())
                .setResource(resource)
                .build();

        return ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(resourceLogs)
                .build();
    }

    private void assertSecureResponseWithStatusCode(final AggregatedHttpResponse response,
                                                    final HttpStatus expectedStatus,
                                                    final Throwable throwable) {
        assertThat("Http Status", response.status(), equalTo(expectedStatus));
        assertThat("Http Response Throwable", throwable, is(nullValue()));

        final List<String> headerKeys = response.headers()
                .stream()
                .map(Map.Entry::getKey)
                .map(AsciiString::toString)
                .map(String::toLowerCase)
                .collect(Collectors.toList());
        assertThat("Response Header Keys", headerKeys, not(hasItem("server")));
    }

    private byte[] createGZipCompressedPayload(final String payload) throws IOException {
        // Create a GZip compressed request body
        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (final GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream)) {
            gzipStream.write(payload.getBytes(StandardCharsets.UTF_8));
        }
        return byteStream.toByteArray();
    }

}
