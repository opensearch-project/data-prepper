/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otellogs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigFixture.createConfigWithBasicAuth;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigFixture.createDefaultConfigBuilder;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigFixture.createJsonHttpPayload;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigFixture.createLogsConfigWithoutSsl;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigFixture.createLogsConfigWittSsl;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigFixture.createLogsServiceRequest;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.BASIC_AUTH_PASSWORD;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.BASIC_AUTH_USERNAME;
import static org.opensearch.dataprepper.plugins.source.otellogs.OtelLogsSourceConfigTestData.CONFIG_PATH;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

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
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.GrpcRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.GrpcBasicAuthenticationProvider;
import org.opensearch.dataprepper.plugins.HttpBasicArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.otel.codec.OTelLogsDecoder;
import org.opensearch.dataprepper.plugins.server.HealthGrpcService;
import org.opensearch.dataprepper.plugins.source.otellogs.certificate.CertificateProviderFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.UnprocessedRequestException;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.RequestHeadersBuilder;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;

import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Statistic;
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

@ExtendWith(MockitoExtension.class)
class OTelLogsSourceHttpTest {
    private static final String GRPC_ENDPOINT = "gproto+http://127.0.0.1:21892/";
    private static final String TEST_PIPELINE_NAME = "test_pipeline";
    private static final String USERNAME = "test_user";
    private static final String PASSWORD = "test_password";
    private static final String TEST_PATH = "${pipelineName}/v1/logs";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

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
    private HttpBasicAuthenticationConfig httpBasicAuthenticationConfig;

    @Mock
    private BlockingBuffer<Record<Object>> buffer;

    @Mock(lenient = true)
    private OTelLogsSourceConfig oTelLogsSourceConfig;

    private PluginSetting pluginSetting;
    private PluginSetting testPluginSetting;
    private PluginMetrics pluginMetrics;
    private PipelineDescription pipelineDescription;
    private OTelLogsSource SOURCE;
    private static final ExportLogsServiceRequest LOGS_REQUEST = ExportLogsServiceRequest.newBuilder()
            .addResourceLogs(ResourceLogs.newBuilder().build()).build();

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

        MetricsTestUtil.initMetrics();
        pluginMetrics = PluginMetrics.fromNames("otel_logs", "pipeline");

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(authenticationProvider);
        pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        SOURCE = new OTelLogsSource(createLogsConfigWithoutSsl(), pluginMetrics, pluginFactory, pipelineDescription);
    }

    @AfterEach
    public void afterEach() {
        SOURCE.stop();
    }

    private void configureObjectUnderTest() {
        SOURCE = new OTelLogsSource(createLogsConfigWithoutSsl(), pluginMetrics, pluginFactory, pipelineDescription);
        assertTrue(SOURCE.getDecoder() instanceof OTelLogsDecoder);
    }

    private RequestHeadersBuilder getDefaultRequestHeadersBuilder() {
        return RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21892") // todo tlongo create constant
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.logs.v1.LogsService/Export")
                        .contentType(MediaType.JSON_UTF_8);
    }

    @ParameterizedTest
    @MethodSource("getPathParams")
    void httpRequest_writesToBuffer_returnsSuccessfulResponse(String givenPath, String resolvedRequestPath) throws Exception {
        OTelLogsSource source = new OTelLogsSource(createDefaultConfigBuilder()
                .path(givenPath)
                .build(), pluginMetrics, pluginFactory, pipelineDescription);
        source.start(buffer);
        ExportLogsServiceRequest request = createExportLogsRequest();

        WebClient.of().execute(
                getDefaultRequestHeadersBuilder().path(resolvedRequestPath).scheme(SessionProtocol.HTTP).build(),
                HttpData.copyOf(JsonFormat.printer().print(request).getBytes())
        )
            .aggregate()
            .whenComplete((response, throwable) -> assertThat(response.status(), is(HttpStatus.OK)))
            .join();

        verify(buffer).writeAll(any(), anyInt());
        source.stop();
    }

    @Test
    void httpsRequest_requestIsProcessed_writesToBufferAndReturnsSuccessfulResponse() throws Exception {
        OTelLogsSource source = new OTelLogsSource(createLogsConfigWittSsl(), pluginMetrics, pluginFactory, pipelineDescription);
        source.start(buffer);
        ExportLogsServiceRequest request = createExportLogsRequest();

        WebClient.builder().factory(ClientFactory.insecure()).build().execute(
                        getDefaultRequestHeadersBuilder().scheme(SessionProtocol.HTTPS).build(),
                        HttpData.copyOf(JsonFormat.printer().print(request).getBytes())
                )
                .aggregate()
                .whenComplete((response, throwable) -> assertThat(response.status(), is(HttpStatus.OK)))
                .join();

        verify(buffer).writeAll(any(), anyInt());
        source.stop();
    }

    private static Stream<Arguments> getPathParams() {
        return Stream.of(
                Arguments.of(CONFIG_PATH, CONFIG_PATH),
                Arguments.of("/${pipelineName}/v1/logs", "/test_pipeline/v1/logs")
        );
    }

    @Test
    // todo tlongo extract into separate test class that deals with general server stuff
    void testServerConnectionsMetric() throws InvalidProtocolBufferException {
        // Prepare
        SOURCE.start(buffer);

        List<Measurement> serverConnectionsMeasurements = MetricsTestUtil.getMeasurementList("pipeline.otel_logs.serverConnections");

        // Verify connections metric value is 0
        Measurement serverConnectionsMeasurement = MetricsTestUtil.getMeasurementFromList(serverConnectionsMeasurements, Statistic.VALUE);
        assertEquals(0, serverConnectionsMeasurement.getValue());

        final RequestHeaders testRequestHeaders = RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21892")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.logs.v1.LogsService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .build();

        // Send request
        WebClient.of().execute(testRequestHeaders, createJsonHttpPayload())
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();

        // Verify connections metric value is 1
        serverConnectionsMeasurement = MetricsTestUtil.getMeasurementFromList(serverConnectionsMeasurements, Statistic.VALUE);
        assertEquals(1.0, serverConnectionsMeasurement.getValue());
    }

    @Test
    // todo tlongo
    void compressedRequest_requestIsProcessed_returns200() throws IOException {
        OTelLogsSource source = new OTelLogsSource(
                createDefaultConfigBuilder().compression(CompressionOption.GZIP).build(),
                pluginMetrics,
                pluginFactory,
                pipelineDescription);
        source.start(buffer);

        WebClient.of().execute(getDefaultRequestHeadersBuilder()
                                .add(HttpHeaderNames.CONTENT_ENCODING, "gzip")
                                .build(),
                        createGZipCompressedPayload(JsonFormat.printer().print(createLogsServiceRequest())))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();
    }

    @Test
    // todo tlongo make parameterized test
    void httpRequestWithBasicAuth_requestIsProcessed_returnsAppropriateResponse() throws Exception {
        final HttpBasicAuthenticationConfig basicAuthConfig = new HttpBasicAuthenticationConfig(BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD);
        final HttpBasicArmeriaHttpAuthenticationProvider authProvider = new HttpBasicArmeriaHttpAuthenticationProvider(basicAuthConfig);
        when(pluginFactory.loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(authProvider);
        final OTelLogsSource source = new OTelLogsSource(createConfigWithBasicAuth(), pluginMetrics, pluginFactory, pipelineDescription);
        source.start(buffer);

        final String encodedCredentials = Base64.getEncoder().encodeToString(String.format("%s:%s", "test", "password").getBytes(StandardCharsets.UTF_8));
        WebClient.of().execute(getDefaultRequestHeadersBuilder()
                                .add(HttpHeaderNames.AUTHORIZATION, "Basic " + encodedCredentials)
                                .build(),
                        HttpData.copyOf(JsonFormat.printer().print(LOGS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();

    }

    @Test
        // todo tlongo make parameterized test
    void httpRequestWithBasicAuth_invalidCredentialsAreProvided_returns403AndDoesNotWriteToBuffer() throws Exception {
        final HttpBasicAuthenticationConfig basicAuthConfig = new HttpBasicAuthenticationConfig(BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD);
        final HttpBasicArmeriaHttpAuthenticationProvider authProvider = new HttpBasicArmeriaHttpAuthenticationProvider(basicAuthConfig);
        when(pluginFactory.loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(authProvider);
        final OTelLogsSource source = new OTelLogsSource(createConfigWithBasicAuth(), pluginMetrics, pluginFactory, pipelineDescription);
        source.start(buffer);

        final String encodedCredentials = Base64.getEncoder().encodeToString(String.format("%s:%s", "test", "wrong_password").getBytes(StandardCharsets.UTF_8));
        WebClient.of().execute(getDefaultRequestHeadersBuilder()
                                .add(HttpHeaderNames.AUTHORIZATION, "Basic " + encodedCredentials)
                                .build(),
                        HttpData.copyOf(JsonFormat.printer().print(LOGS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.UNAUTHORIZED, throwable))
                .join();

        verify(buffer, never()).writeAll(any(), anyInt());
    }


    @ParameterizedTest
    @MethodSource("getHealthCheckParams")
    void healthCheckRequest_requestIsProcesses_returnsStatusCodeAccordingToConfig(boolean givenHealthCheckConfig, HttpStatus expectedStatus) throws IOException {
        final OTelLogsSource source = new OTelLogsSource(createDefaultConfigBuilder()
                .healthCheck(givenHealthCheckConfig).build(), pluginMetrics, pluginFactory, pipelineDescription);
        source.start(buffer);

        WebClient.of().execute(getDefaultRequestHeadersBuilder()
                        .path("/health")
                                .method(HttpMethod.GET)
                                .build(),
                        HttpData.copyOf(JsonFormat.printer().print(LOGS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, expectedStatus, throwable))
                .join();
        source.stop();
    }

    private static Stream<Arguments> getHealthCheckParams() {
        return Stream.of(
                Arguments.of(true, HttpStatus.OK),
                Arguments.of(false, HttpStatus.NOT_FOUND)
        );
    }

    @Test
    void testStartWithEmptyBuffer() {
        assertThrows(IllegalStateException.class, () -> SOURCE.start(null));
    }

    @Test
    // todo tlongo
    void testStartWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
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
    void testStartWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
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
    void testStopWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
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
    void testStartWithInterruptedException() throws ExecutionException, InterruptedException {
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
    void testStopWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
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
    void testStopWithInterruptedException() throws ExecutionException, InterruptedException {
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


    @ParameterizedTest
    @ArgumentsSource(BufferExceptionToStatusArgumentsProvider.class)
    // todo tlongo
    void gRPC_request_returns_expected_status_for_exceptions_from_buffer(
            final Class<Exception> bufferExceptionClass,
            final Status.Code expectedStatusCode) throws Exception {
        configureObjectUnderTest();
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

    @Test
    void httpRequest_requestBodyIsTooLarge_returns413() throws InvalidProtocolBufferException {
        OTelLogsSource source = new OTelLogsSource(createDefaultConfigBuilder().maxRequestLength(ByteCount.ofBytes(4)).build(), pluginMetrics, pluginFactory, pipelineDescription);
        source.start(buffer);

        WebClient.of()
                .execute(getDefaultRequestHeadersBuilder().build(), createJsonHttpPayload())
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.REQUEST_ENTITY_TOO_LARGE, throwable))
                .join();
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
