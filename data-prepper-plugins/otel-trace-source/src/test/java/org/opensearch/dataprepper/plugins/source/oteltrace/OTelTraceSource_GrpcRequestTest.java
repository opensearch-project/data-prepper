/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.oteltrace;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_PORT;
import static org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.GrpcRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.GrpcBasicAuthenticationProvider;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.source.oteltrace.certificate.CertificateProviderFactory;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.client.Clients;
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
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.netty.util.AsciiString;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;

@ExtendWith(MockitoExtension.class)
class OTelTraceSource_GrpcRequestTest {
    private static final String GRPC_ENDPOINT = "gproto+http://127.0.0.1:21890/";
    private static final String USERNAME = "test_user";
    private static final String PASSWORD = "test_password";
    private static final String TEST_PATH = "${pipelineName}/v1/traces";
    private static final String TEST_PIPELINE_NAME = "test_pipeline";
    private static final RetryInfoConfig TEST_RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(50), Duration.ofMillis(2000));

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

    @Mock(lenient = true)
    private OTelTraceSourceConfig oTelTraceSourceConfig;

    @Mock
    private Buffer<Record<Object>> buffer;

    @Mock
    private HttpBasicAuthenticationConfig httpBasicAuthenticationConfig;


    private PluginMetrics pluginMetrics;
    private PipelineDescription pipelineDescription;
    private OTelTraceSource SOURCE;

    @BeforeEach
    void beforeEach() {
        lenient().when(serverBuilder.port(anyInt(), ArgumentMatchers.<SessionProtocol>any())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.service(any(GrpcService.class))).thenReturn(serverBuilder);
        lenient().when(serverBuilder.service(any(GrpcService.class), any(Function.class))).thenReturn(serverBuilder);
        lenient().when(serverBuilder.http(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.https(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);

        lenient().when(server.start()).thenReturn(completableFuture);

        lenient().when(grpcServiceBuilder.addService(any(BindableService.class))).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useClientTimeoutHeader(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useBlockingTaskExecutor(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.exceptionHandler(any(
                GrpcRequestExceptionHandler.class))).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.build()).thenReturn(grpcService);

        lenient().when(authenticationProvider.getHttpAuthenticationService()).thenCallRealMethod();

        when(oTelTraceSourceConfig.getPort()).thenReturn(DEFAULT_PORT);
        when(oTelTraceSourceConfig.isSsl()).thenReturn(false);
        when(oTelTraceSourceConfig.getRequestTimeoutInMillis()).thenReturn(DEFAULT_REQUEST_TIMEOUT_MS);
        when(oTelTraceSourceConfig.getMaxConnectionCount()).thenReturn(10);
        when(oTelTraceSourceConfig.getThreadCount()).thenReturn(5);
        when(oTelTraceSourceConfig.getCompression()).thenReturn(CompressionOption.NONE);
        when(oTelTraceSourceConfig.getRetryInfo()).thenReturn(TEST_RETRY_INFO);

        lenient().when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(authenticationProvider);
        configureObjectUnderTest();
        pipelineDescription = mock(PipelineDescription.class);
        lenient().when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
    }

    @AfterEach
    void afterEach() {
        SOURCE.stop();
    }

    private void configureObjectUnderTest() {
        MetricsTestUtil.initMetrics();
        pluginMetrics = PluginMetrics.fromNames("otel_trace", "pipeline");

        pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        SOURCE = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
    }


    @Test
    void gRPC_request_writes_to_buffer_with_successful_response() throws Exception {
        configureObjectUnderTest();
        SOURCE.start(buffer);

        final TraceServiceGrpc.TraceServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .build(TraceServiceGrpc.TraceServiceBlockingStub.class);
        final ExportTraceServiceResponse exportResponse = client.export(createExportTraceRequest());
        assertThat(exportResponse, notNullValue());

        final ArgumentCaptor<Collection<Record<Object>>> bufferWriteArgumentCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(buffer).writeAll(bufferWriteArgumentCaptor.capture(), anyInt());

        final Collection<Record<Object>> actualBufferWrites = bufferWriteArgumentCaptor.getValue();
        assertThat(actualBufferWrites, notNullValue());
        assertThat(actualBufferWrites, hasSize(1));
    }

    @Test
    void gRPC_with_auth_request_writes_to_buffer_with_successful_response() throws Exception {
        when(httpBasicAuthenticationConfig.getUsername()).thenReturn(USERNAME);
        when(httpBasicAuthenticationConfig.getPassword()).thenReturn(PASSWORD);
        final GrpcAuthenticationProvider grpcAuthenticationProvider = new GrpcBasicAuthenticationProvider(httpBasicAuthenticationConfig);

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(grpcAuthenticationProvider);
        when(oTelTraceSourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelTraceSourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                        "username", USERNAME,
                        "password", PASSWORD
                )));
        configureObjectUnderTest();
        SOURCE.start(buffer);

        final String encodeToString = Base64.getEncoder()
                .encodeToString(String.format("%s:%s", USERNAME, PASSWORD).getBytes(StandardCharsets.UTF_8));

        final TraceServiceGrpc.TraceServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .addHeader("Authorization", "Basic " + encodeToString)
                .build(TraceServiceGrpc.TraceServiceBlockingStub.class);
        final ExportTraceServiceResponse exportResponse = client.export(createExportTraceRequest());
        assertThat(exportResponse, notNullValue());

        final ArgumentCaptor<Collection<Record<Object>>> bufferWriteArgumentCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(buffer).writeAll(bufferWriteArgumentCaptor.capture(), anyInt());

        final Collection<Record<Object>> actualBufferWrites = bufferWriteArgumentCaptor.getValue();
        assertThat(actualBufferWrites, notNullValue());
        assertThat(actualBufferWrites, hasSize(1));
    }

    @Test
    void gRPC_request_with_custom_path_throws_when_written_to_default_path() {
        when(oTelTraceSourceConfig.getPath()).thenReturn(TEST_PATH);
        when(oTelTraceSourceConfig.enableUnframedRequests()).thenReturn(true);

        configureObjectUnderTest();
        SOURCE.start(buffer);

        final TraceServiceGrpc.TraceServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .build(TraceServiceGrpc.TraceServiceBlockingStub.class);

        final StatusRuntimeException actualException = assertThrows(StatusRuntimeException.class, () -> client.export(createExportTraceRequest()));
        assertThat(actualException.getStatus(), notNullValue());
        assertThat(actualException.getMessage(), actualException.getStatus().getCode(), equalTo(Status.UNIMPLEMENTED.getCode()));
    }

    @ParameterizedTest
    @ArgumentsSource(BufferExceptionToStatusArgumentsProvider.class)
    void gRPC_request_returns_expected_status_for_exceptions_from_buffer(
            final Class<Exception> bufferExceptionClass,
            final Status.Code expectedStatusCode) throws Exception {
        configureObjectUnderTest();
        SOURCE.start(buffer);

        final TraceServiceGrpc.TraceServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .build(TraceServiceGrpc.TraceServiceBlockingStub.class);

        doThrow(bufferExceptionClass)
                .when(buffer)
                .writeAll(anyCollection(), anyInt());
        final ExportTraceServiceRequest exportTraceRequest = createExportTraceRequest();
        final StatusRuntimeException actualException = assertThrows(StatusRuntimeException.class, () -> client.export(exportTraceRequest));

        assertThat(actualException.getStatus(), notNullValue());
        assertThat(actualException.getMessage(), actualException.getStatus().getCode(), equalTo(expectedStatusCode));
    }

    @Test
    void gRPC_request_throws_InvalidArgument_for_malformed_trace_data() {
        configureObjectUnderTest();
        SOURCE.start(buffer);

        final TraceServiceGrpc.TraceServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .build(TraceServiceGrpc.TraceServiceBlockingStub.class);

        final ExportTraceServiceRequest exportTraceRequest = createInvalidExportTraceRequest();
        final StatusRuntimeException actualException = assertThrows(StatusRuntimeException.class, () -> client.export(exportTraceRequest));

        assertThat(actualException.getStatus(), notNullValue());
        assertThat(actualException.getMessage(), actualException.getStatus().getCode(), equalTo(Status.Code.INVALID_ARGUMENT));

        verifyNoInteractions(buffer);
    }

    @Test
    void request_that_exceeds_maxRequestLength_returns_413() throws InvalidProtocolBufferException {
        when(oTelTraceSourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelTraceSourceConfig.getMaxRequestLength()).thenReturn(ByteCount.ofBytes(4));
        configureObjectUnderTest();
        SOURCE.start(buffer);

        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority("127.0.0.1:21890")
                                .method(HttpMethod.POST)
                                .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.copyOf(JsonFormat.printer().print(createExportTraceRequest()).getBytes()))
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

    private ExportTraceServiceRequest createInvalidExportTraceRequest() {
        final Span testSpan = Span.newBuilder()
                .setTraceState("SUCCESS").build();
        final ExportTraceServiceRequest successRequest = ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(ResourceSpans.newBuilder()
                        .addScopeSpans(ScopeSpans.newBuilder().addSpans(testSpan)).build())
                .build();

        return successRequest;
    }

    private ExportTraceServiceRequest createExportTraceRequest() {
        final Span testSpan = Span.newBuilder()
                .setTraceId(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .setSpanId(ByteString.copyFromUtf8(UUID.randomUUID().toString()))
                .setName(UUID.randomUUID().toString())
                .setKind(Span.SpanKind.SPAN_KIND_SERVER)
                .setStartTimeUnixNano(100)
                .setEndTimeUnixNano(101)
                .setTraceState("SUCCESS").build();

        return ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(ResourceSpans.newBuilder()
                        .addScopeSpans(ScopeSpans.newBuilder().addSpans(testSpan)).build())
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
                .collect(Collectors.toList());
        assertThat("Response Header Keys", headerKeys, not(contains("server")));
    }
}
