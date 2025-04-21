package org.opensearch.dataprepper.plugins.source.oteltelemetry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.GrpcRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.GrpcBasicAuthenticationProvider;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.source.otelmetrics.certificate.CertificateProviderFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.Clients;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.ClosedSessionException;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
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
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.LogsServiceGrpc;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
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
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.oteltelemetry.OTelTelemetrySourceConfig.DEFAULT_PORT;
import static org.opensearch.dataprepper.plugins.source.oteltelemetry.OTelTelemetrySourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;
import static org.opensearch.dataprepper.plugins.source.oteltelemetry.OTelTelemetrySourceConfig.SSL;

@ExtendWith(MockitoExtension.class)
class OTelTelemetrySourceTest {
    private static final String GRPC_ENDPOINT = "gproto+http://127.0.0.1:21893/";
    private static final String USERNAME = "test_user";
    private static final String PASSWORD = "test_password";
    private static final String LOGS_TEST_PATH = "/v1/logs";
    private static final String METRICS_TEST_PATH = "/v1/metrics";
    private static final String TRACES_TEST_PATH = "/v1/traces";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final String TEST_PIPELINE_NAME = "test_pipeline";
    private static final RetryInfoConfig TEST_RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(50),
            Duration.ofMillis(2000));

    private static final ExportLogsServiceRequest LOGS_REQUEST = ExportLogsServiceRequest.newBuilder()
            .addResourceLogs(ResourceLogs.newBuilder().build()).build();
    private static final ExportMetricsServiceRequest METRICS_REQUEST = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(ResourceMetrics.newBuilder().build()).build();
    private static final ExportTraceServiceRequest TRACES_SUCCESS_REQUEST = ExportTraceServiceRequest.newBuilder()
            .addResourceSpans(ResourceSpans.newBuilder()
                    .addScopeSpans(ScopeSpans.newBuilder()
                            .addSpans(
                                    io.opentelemetry.proto.trace.v1.Span.newBuilder().setTraceState("SUCCESS").build()))
                    .build())
            .build();
    private static final ExportTraceServiceRequest TRACES_FAILURE_REQUEST = ExportTraceServiceRequest.newBuilder()
            .addResourceSpans(ResourceSpans.newBuilder()
                    .addScopeSpans(ScopeSpans.newBuilder()
                            .addSpans(
                                    io.opentelemetry.proto.trace.v1.Span.newBuilder().setTraceState("FAILURE").build()))
                    .build())
            .build();

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
    private OTelTelemetrySourceConfig oTelTelemetrySourceConfig;

    @Mock
    private BlockingBuffer<Record<Object>> buffer;

    @Mock
    private HttpBasicAuthenticationConfig httpBasicAuthenticationConfig;

    private PluginSetting pluginSetting;
    private PluginSetting testPluginSetting;
    private PluginMetrics pluginMetrics;
    private PipelineDescription pipelineDescription;
    private OTelTelemetrySource SOURCE;

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
        lenient().when(grpcServiceBuilder.exceptionHandler(any(
                GrpcRequestExceptionHandler.class))).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.build()).thenReturn(grpcService);
        MetricsTestUtil.initMetrics();
        pluginMetrics = PluginMetrics.fromNames("otel_telemetry", "pipeline");

        when(oTelTelemetrySourceConfig.getPort()).thenReturn(DEFAULT_PORT);
        when(oTelTelemetrySourceConfig.isSsl()).thenReturn(false);
        when(oTelTelemetrySourceConfig.getRequestTimeoutInMillis()).thenReturn(DEFAULT_REQUEST_TIMEOUT_MS);
        when(oTelTelemetrySourceConfig.getMaxConnectionCount()).thenReturn(10);
        when(oTelTelemetrySourceConfig.getThreadCount()).thenReturn(5);
        when(oTelTelemetrySourceConfig.getCompression()).thenReturn(CompressionOption.NONE);

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(authenticationProvider);
        pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        SOURCE = new OTelTelemetrySource(oTelTelemetrySourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
    }

    @AfterEach
    public void afterEach() {
        SOURCE.stop();
    }

    private void configureObjectUnderTest() {
        SOURCE = new OTelTelemetrySource(oTelTelemetrySourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
    }

    @Test
    void testHttpFullJsonWithNonUnframedRequests() throws InvalidProtocolBufferException {
        configureObjectUnderTest();
        SOURCE.start(buffer);
        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .build(),
                HttpData.copyOf(JsonFormat.printer().print(TRACES_SUCCESS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();
        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .build(),
                HttpData.copyOf(JsonFormat.printer().print(TRACES_FAILURE_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();

        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .build(),
                HttpData.copyOf(JsonFormat.printer().print(METRICS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();

        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.logs.v1.LogsService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .build(),
                HttpData.copyOf(JsonFormat.printer().print(LOGS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();
    }

    @Test
    void testHttpsFullJsonWithNonUnframedRequests() throws InvalidProtocolBufferException {

        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("request_timeout", 5);
        settingsMap.put(SSL, true);
        settingsMap.put("useAcmCertForSSL", false);
        settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
        settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");
        pluginSetting = new PluginSetting("otel_telemetry", settingsMap);
        pluginSetting.setPipelineName("pipeline");

        oTelTelemetrySourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(),
                OTelTelemetrySourceConfig.class);
        SOURCE = new OTelTelemetrySource(oTelTelemetrySourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        SOURCE.start(buffer);

        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTPS)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .build(),
                HttpData.copyOf(JsonFormat.printer().print(TRACES_SUCCESS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();
        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTPS)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .build(),
                HttpData.copyOf(JsonFormat.printer().print(TRACES_FAILURE_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();

        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTPS)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .build(),
                HttpData.copyOf(JsonFormat.printer().print(METRICS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();

        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTPS)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.logs.v1.LogsService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .build(),
                HttpData.copyOf(JsonFormat.printer().print(LOGS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();
    }

    @Test
    void testHttpFullBytesWithNonUnframedRequests() {
        configureObjectUnderTest();
        SOURCE.start(buffer);
        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                .contentType(MediaType.PROTOBUF)
                .build(),
                HttpData.copyOf(TRACES_SUCCESS_REQUEST.toByteArray()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();
        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                .contentType(MediaType.PROTOBUF)
                .build(),
                HttpData.copyOf(TRACES_FAILURE_REQUEST.toByteArray()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();

        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
                .contentType(MediaType.PROTOBUF)
                .build(),
                HttpData.copyOf(METRICS_REQUEST.toByteArray()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();

        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.logs.v1.LogsService/Export")
                .contentType(MediaType.PROTOBUF)
                .build(),
                HttpData.copyOf(LOGS_REQUEST.toByteArray()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();
    }

    @Test
    void testHttpFullJsonWithUnframedRequests() throws InvalidProtocolBufferException {
        when(oTelTelemetrySourceConfig.enableUnframedRequests()).thenReturn(true);
        configureObjectUnderTest();
        SOURCE.start(buffer);

        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .build(),
                HttpData.copyOf(JsonFormat.printer().print(createExportTraceRequest()).getBytes()))
                .aggregate()
                .whenComplete(
                        (response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();

        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .build(),
                HttpData.copyOf(JsonFormat.printer().print(METRICS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete(
                        (response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();

        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.logs.v1.LogsService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .build(),
                HttpData.copyOf(JsonFormat.printer().print(LOGS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete(
                        (response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();
    }

    @Test
    void testHttpCompressionWithUnframedRequests() throws IOException {
        when(oTelTelemetrySourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelTelemetrySourceConfig.getCompression()).thenReturn(CompressionOption.GZIP);
        configureObjectUnderTest();
        SOURCE.start(buffer);

        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .add(HttpHeaderNames.CONTENT_ENCODING, "gzip")
                .build(),
                createGZipCompressedPayload(JsonFormat.printer().print(createExportTraceRequest())))
                .aggregate()
                .whenComplete(
                        (response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();

        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .add(HttpHeaderNames.CONTENT_ENCODING, "gzip")
                .build(),
                createGZipCompressedPayload(JsonFormat.printer().print(METRICS_REQUEST)))
                .aggregate()
                .whenComplete(
                        (response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();

        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.logs.v1.LogsService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .add(HttpHeaderNames.CONTENT_ENCODING, "gzip")
                .build(),
                createGZipCompressedPayload(JsonFormat.printer().print(LOGS_REQUEST)))
                .aggregate()
                .whenComplete(
                        (response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();
    }

    @Test
    void testHttpFullJsonWithCustomPathAndUnframedRequests() throws InvalidProtocolBufferException {
        when(oTelTelemetrySourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelTelemetrySourceConfig.getLogsPath()).thenReturn(LOGS_TEST_PATH);
        when(oTelTelemetrySourceConfig.getMetricsPath()).thenReturn(METRICS_TEST_PATH);
        when(oTelTelemetrySourceConfig.getTracesPath()).thenReturn(TRACES_TEST_PATH);
        configureObjectUnderTest();
        SOURCE.start(buffer);

        final String transformedTracesPath = "/" + TEST_PIPELINE_NAME + "/v1/traces";
        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path(transformedTracesPath)
                .contentType(MediaType.JSON_UTF_8)
                .build(),
                HttpData.copyOf(JsonFormat.printer().print(createExportTraceRequest()).getBytes()))
                .aggregate()
                .whenComplete(
                        (response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();

        final String transformedMetricsPath = "/" + TEST_PIPELINE_NAME + "/v1/metrics";
        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path(transformedMetricsPath)
                .contentType(MediaType.JSON_UTF_8)
                .build(),
                HttpData.copyOf(JsonFormat.printer().print(METRICS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete(
                        (response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();

        final String transformedLogsPath = "/" + TEST_PIPELINE_NAME + "/v1/logs";
        WebClient.of().execute(RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21893")
                .method(HttpMethod.POST)
                .path(transformedLogsPath)
                .contentType(MediaType.JSON_UTF_8)
                .build(),
                HttpData.copyOf(JsonFormat.printer().print(LOGS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete(
                        (response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();
    }

    @Test
    void testHttpFullJsonWithCustomPathAndAuthHeader_with_successful_response() throws InvalidProtocolBufferException {
        when(httpBasicAuthenticationConfig.getUsername()).thenReturn(USERNAME);
        when(httpBasicAuthenticationConfig.getPassword()).thenReturn(PASSWORD);
        final GrpcAuthenticationProvider grpcAuthenticationProvider = new GrpcBasicAuthenticationProvider(
                httpBasicAuthenticationConfig);

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(grpcAuthenticationProvider);
        when(oTelTelemetrySourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                        "username", USERNAME,
                        "password", PASSWORD)));
        when(oTelTelemetrySourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelTelemetrySourceConfig.getLogsPath()).thenReturn(LOGS_TEST_PATH);
        when(oTelTelemetrySourceConfig.getMetricsPath()).thenReturn(METRICS_TEST_PATH);
        when(oTelTelemetrySourceConfig.getTracesPath()).thenReturn(TRACES_TEST_PATH);

        configureObjectUnderTest();
        SOURCE.start(buffer);

        final String encodeToString = Base64.getEncoder()
                .encodeToString(String.format("%s:%s", USERNAME, PASSWORD).getBytes(StandardCharsets.UTF_8));

        final String transformedTracesPath = "/" + TEST_PIPELINE_NAME + "/v1/traces";
        WebClient.of().prepare()
                .post("http://127.0.0.1:21893" + transformedTracesPath)
                .content(MediaType.JSON_UTF_8, JsonFormat.printer().print(createExportTraceRequest()).getBytes())
                .header("Authorization", "Basic " + encodeToString)
                .execute()
                .aggregate()
                .whenComplete(
                        (response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();

        final String transformedMetricsPath = "/" + TEST_PIPELINE_NAME + "/v1/metrics";
        WebClient.of().prepare()
                .post("http://127.0.0.1:21893" + transformedMetricsPath)
                .content(MediaType.JSON_UTF_8, JsonFormat.printer().print(createExportMetricsRequest()).getBytes())
                .header("Authorization", "Basic " + encodeToString)
                .execute()
                .aggregate()
                .whenComplete(
                        (response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();

        final String transformedLogsPath = "/" + TEST_PIPELINE_NAME + "/v1/logs";
        WebClient.of().prepare()
                .post("http://127.0.0.1:21893" + transformedLogsPath)
                .content(MediaType.JSON_UTF_8, JsonFormat.printer().print(createExportLogsRequest()).getBytes())
                .header("Authorization", "Basic " + encodeToString)
                .execute()
                .aggregate()
                .whenComplete(
                        (response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();
    }

    @Test
    void testHttpFullJsonWithCustomPathAndAuthHeader_with_unsuccessful_response()
            throws InvalidProtocolBufferException {
        when(httpBasicAuthenticationConfig.getUsername()).thenReturn(USERNAME);
        when(httpBasicAuthenticationConfig.getPassword()).thenReturn(PASSWORD);
        final GrpcAuthenticationProvider grpcAuthenticationProvider = new GrpcBasicAuthenticationProvider(
                httpBasicAuthenticationConfig);

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(grpcAuthenticationProvider);
        when(oTelTelemetrySourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                        "username", USERNAME,
                        "password", PASSWORD)));
        when(oTelTelemetrySourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelTelemetrySourceConfig.getLogsPath()).thenReturn(LOGS_TEST_PATH);
        when(oTelTelemetrySourceConfig.getMetricsPath()).thenReturn(METRICS_TEST_PATH);
        when(oTelTelemetrySourceConfig.getTracesPath()).thenReturn(TRACES_TEST_PATH);

        configureObjectUnderTest();
        SOURCE.start(buffer);

        final String transformedTracesPath = "/" + TEST_PIPELINE_NAME + "/v1/traces";
        WebClient.of().prepare()
                .post("http://127.0.0.1:21893" + transformedTracesPath)
                .content(MediaType.JSON_UTF_8, JsonFormat.printer().print(createExportTraceRequest()).getBytes())
                .execute()
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNAUTHORIZED, throwable))
                .join();

        final String transformedMetricsPath = "/" + TEST_PIPELINE_NAME + "/v1/metrics";
        WebClient.of().prepare()
                .post("http://127.0.0.1:21893" + transformedMetricsPath)
                .content(MediaType.JSON_UTF_8, JsonFormat.printer().print(createExportMetricsRequest()).getBytes())
                .execute()
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNAUTHORIZED, throwable))
                .join();

        final String transformedLogsPath = "/" + TEST_PIPELINE_NAME + "/v1/logs";
        WebClient.of().prepare()
                .post("http://127.0.0.1:21893" + transformedLogsPath)
                .content(MediaType.JSON_UTF_8, JsonFormat.printer().print(createExportLogsRequest()).getBytes())
                .execute()
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNAUTHORIZED, throwable))
                .join();
    }

    @Test
    void testHttpRequestWithInvalidCredentialsShouldReturnUnauthorized() throws InvalidProtocolBufferException {
        when(httpBasicAuthenticationConfig.getUsername()).thenReturn(USERNAME);
        when(httpBasicAuthenticationConfig.getPassword()).thenReturn(PASSWORD);
        final GrpcAuthenticationProvider grpcAuthenticationProvider = new GrpcBasicAuthenticationProvider(
                httpBasicAuthenticationConfig);

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(grpcAuthenticationProvider);
        when(oTelTelemetrySourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                        "username", USERNAME,
                        "password", PASSWORD)));
        when(oTelTelemetrySourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelTelemetrySourceConfig.getLogsPath()).thenReturn(LOGS_TEST_PATH);
        when(oTelTelemetrySourceConfig.getMetricsPath()).thenReturn(METRICS_TEST_PATH);
        when(oTelTelemetrySourceConfig.getTracesPath()).thenReturn(TRACES_TEST_PATH);
        configureObjectUnderTest();
        SOURCE.start(buffer);

        final String invalidUsername = "wrong_user";
        final String invalidPassword = "wrong_password";
        final String invalidCredentials = Base64.getEncoder()
                .encodeToString(
                        String.format("%s:%s", invalidUsername, invalidPassword).getBytes(StandardCharsets.UTF_8));

        final String transformedTracesPath = "/" + TEST_PIPELINE_NAME + "/v1/traces";
        WebClient.of().prepare()
                .post("http://127.0.0.1:21893" + transformedTracesPath)
                .content(MediaType.JSON_UTF_8, JsonFormat.printer().print(createExportTraceRequest()).getBytes())
                .header("Authorization", "Basic " + invalidCredentials)
                .execute()
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNAUTHORIZED, throwable))
                .join();

        final String transformedMetricsPath = "/" + TEST_PIPELINE_NAME + "/v1/metrics";
        WebClient.of().prepare()
                .post("http://127.0.0.1:21893" + transformedMetricsPath)
                .content(MediaType.JSON_UTF_8, JsonFormat.printer().print(createExportMetricsRequest()).getBytes())
                .header("Authorization", "Basic " + invalidCredentials)
                .execute()
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNAUTHORIZED, throwable))
                .join();

        final String transformedLogsPath = "/" + TEST_PIPELINE_NAME + "/v1/logs";
        WebClient.of().prepare()
                .post("http://127.0.0.1:21893" + transformedLogsPath)
                .content(MediaType.JSON_UTF_8, JsonFormat.printer().print(createExportLogsRequest()).getBytes())
                .header("Authorization", "Basic " + invalidCredentials)
                .execute()
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response,
                        HttpStatus.UNAUTHORIZED, throwable))
                .join();
    }

    @Test
    void testGrpcRequestWithoutAuthentication_with_unsuccessful_response() {
        when(httpBasicAuthenticationConfig.getUsername()).thenReturn(USERNAME);
        when(httpBasicAuthenticationConfig.getPassword()).thenReturn(PASSWORD);
        final GrpcAuthenticationProvider grpcAuthenticationProvider = new GrpcBasicAuthenticationProvider(
                httpBasicAuthenticationConfig);

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(grpcAuthenticationProvider);
        when(oTelTelemetrySourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                        "username", USERNAME,
                        "password", PASSWORD)));
        configureObjectUnderTest();
        SOURCE.start(buffer);

        final TraceServiceGrpc.TraceServiceBlockingStub tracesClient = Clients.builder(GRPC_ENDPOINT)
                .build(TraceServiceGrpc.TraceServiceBlockingStub.class);
        final StatusRuntimeException tracesActualException = assertThrows(StatusRuntimeException.class,
                () -> tracesClient.export(createExportTraceRequest()));
        assertThat(tracesActualException.getStatus(), notNullValue());
        assertThat(tracesActualException.getStatus().getCode(), equalTo(Status.Code.UNAUTHENTICATED));

        final MetricsServiceGrpc.MetricsServiceBlockingStub metricsClient = Clients.builder(GRPC_ENDPOINT)
                .build(MetricsServiceGrpc.MetricsServiceBlockingStub.class);
        final StatusRuntimeException metricsActualException = assertThrows(StatusRuntimeException.class,
                () -> metricsClient.export(createExportMetricsRequest()));
        assertThat(metricsActualException.getStatus(), notNullValue());
        assertThat(metricsActualException.getStatus().getCode(), equalTo(Status.Code.UNAUTHENTICATED));

        final LogsServiceGrpc.LogsServiceBlockingStub logsClient = Clients.builder(GRPC_ENDPOINT)
                .build(LogsServiceGrpc.LogsServiceBlockingStub.class);
        final StatusRuntimeException logsActualException = assertThrows(StatusRuntimeException.class,
                () -> logsClient.export(createExportLogsRequest()));
        assertThat(logsActualException.getStatus(), notNullValue());
        assertThat(logsActualException.getStatus().getCode(), equalTo(Status.Code.UNAUTHENTICATED));
    }

    @Test
    void testHttpWithoutSslFailsWhenSslIsEnabled() throws InvalidProtocolBufferException {
        when(oTelTelemetrySourceConfig.isSsl()).thenReturn(true);
        when(oTelTelemetrySourceConfig.getSslKeyCertChainFile()).thenReturn("data/certificate/test_cert.crt");
        when(oTelTelemetrySourceConfig.getSslKeyFile()).thenReturn("data/certificate/test_decrypted_key.key");
        configureObjectUnderTest();
        SOURCE.start(buffer);

        WebClient client = WebClient.builder("http://127.0.0.1:21893")
                .build();

        CompletionException tracesException = assertThrows(CompletionException.class,
                () -> client.execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21893")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                        HttpData.copyOf(JsonFormat.printer().print(createExportTraceRequest()).getBytes()))
                        .aggregate()
                        .join());
        assertThat(tracesException.getCause(), instanceOf(ClosedSessionException.class));

        CompletionException metricsException = assertThrows(CompletionException.class,
                () -> client.execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21893")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                        HttpData.copyOf(JsonFormat.printer().print(createExportMetricsRequest()).getBytes()))
                        .aggregate()
                        .join());
        assertThat(metricsException.getCause(), instanceOf(ClosedSessionException.class));

        CompletionException logsException = assertThrows(CompletionException.class,
                () -> client.execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21893")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.logs.v1.LogsService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                        HttpData.copyOf(JsonFormat.printer().print(createExportLogsRequest()).getBytes()))
                        .aggregate()
                        .join());
        assertThat(logsException.getCause(), instanceOf(ClosedSessionException.class));
    }

    @Test
    void testGrpcFailsIfSslIsEnabledAndNoTls() {
        when(oTelTelemetrySourceConfig.isSsl()).thenReturn(true);
        when(oTelTelemetrySourceConfig.getSslKeyCertChainFile()).thenReturn("data/certificate/test_cert.crt");
        when(oTelTelemetrySourceConfig.getSslKeyFile()).thenReturn("data/certificate/test_decrypted_key.key");
        configureObjectUnderTest();
        SOURCE.start(buffer);

        TraceServiceGrpc.TraceServiceBlockingStub tracesClient = Clients.builder(GRPC_ENDPOINT)
                .build(TraceServiceGrpc.TraceServiceBlockingStub.class);

        StatusRuntimeException tracesActualException = assertThrows(StatusRuntimeException.class,
                () -> tracesClient.export(createExportTraceRequest()));

        assertThat(tracesActualException.getStatus(), notNullValue());
        assertThat(tracesActualException.getStatus().getCode(), equalTo(Status.Code.UNKNOWN));

        final MetricsServiceGrpc.MetricsServiceBlockingStub metricsClient = Clients.builder(GRPC_ENDPOINT)
                .build(MetricsServiceGrpc.MetricsServiceBlockingStub.class);
        final StatusRuntimeException metricsActualException = assertThrows(StatusRuntimeException.class,
                () -> metricsClient.export(createExportMetricsRequest()));
        assertThat(metricsActualException.getStatus(), notNullValue());
        assertThat(metricsActualException.getStatus().getCode(), equalTo(Status.Code.UNKNOWN));

        final LogsServiceGrpc.LogsServiceBlockingStub logsClient = Clients.builder(GRPC_ENDPOINT)
                .build(LogsServiceGrpc.LogsServiceBlockingStub.class);

        final StatusRuntimeException logsActualException = assertThrows(StatusRuntimeException.class,
                () -> logsClient.export(createExportLogsRequest()));

        assertThat(logsActualException.getStatus(), notNullValue());
        assertThat(logsActualException.getStatus().getCode(), equalTo(Status.Code.UNKNOWN));

    }

    // @Test
    // void testServerStartCertFileSuccess() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testServerStartACMCertSuccess() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void
    // start_with_Health_configured_unframed_requests_includes_HTTPHealthCheck_service()
    // {
    // // TODO: Implement test logic
    // }

    // @Test
    // void start_without_Health_configured_does_not_include_HealthCheck_service() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void
    // start_without_Health_configured_unframed_requests_does_not_include_HealthCheck_service()
    // {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testHealthCheckUnauthNotAllowed() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testHealthCheckUnauthAllowed() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testOptionalHttpAuthServiceNotInPlace() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testOptionalHttpAuthServiceInPlace() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testOptionalHttpAuthServiceInPlaceWithUnauthenticatedDisabled() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testDoubleStart() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testRunAnotherSourceWithSamePort() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testStartWithEmptyBuffer() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testStartWithServerExecutionExceptionNoCause() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testStartWithServerExecutionExceptionWithCause() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testStopWithServerExecutionExceptionNoCause() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testStartWithInterruptedException() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testStopWithServerExecutionExceptionWithCause() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testStopWithInterruptedException() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void gRPC_request_writes_to_buffer_with_successful_response() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void gRPC_with_auth_request_writes_to_buffer_with_successful_response() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void gRPC_request_with_custom_path_throws_when_written_to_default_path() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void gRPC_request_returns_expected_status_for_exceptions_from_buffer() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void gRPC_request_throws_InvalidArgument_for_malformed_trace_data() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void request_that_exceeds_maxRequestLength_returns_413() {
    // // TODO: Implement test logic
    // }

    // @Test
    // void testServerConnectionsMetric() {
    // // TODO: Implement test logic
    // }

    static class BufferExceptionToStatusArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(TimeoutException.class, Status.Code.RESOURCE_EXHAUSTED),
                    arguments(SizeOverflowException.class, Status.Code.RESOURCE_EXHAUSTED),
                    arguments(Exception.class, Status.Code.INTERNAL),
                    arguments(RuntimeException.class, Status.Code.INTERNAL));
        }
    }

    private ExportLogsServiceRequest createExportLogsRequest() {
        final Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue("service").build()))
                .build();

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

    private ExportMetricsServiceRequest createExportMetricsRequest() {
        final Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue("service").build()))
                .build();
        NumberDataPoint.Builder p1 = NumberDataPoint.newBuilder().setAsInt(4);
        Gauge gauge = Gauge.newBuilder().addDataPoints(p1).build();

        io.opentelemetry.proto.metrics.v1.Metric.Builder metric = io.opentelemetry.proto.metrics.v1.Metric.newBuilder()
                .setGauge(gauge)
                .setUnit("seconds")
                .setName("name")
                .setDescription("description");
        ScopeMetrics scopeMetrics = ScopeMetrics.newBuilder()
                .addMetrics(metric)
                .setScope(InstrumentationScope.newBuilder()
                        .setName("ilname")
                        .setVersion("ilversion")
                        .build())
                .build();

        final ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder()
                .setResource(resource)
                .addScopeMetrics(scopeMetrics)
                .build();

        return ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(resourceMetrics).build();
    }

    private ExportTraceServiceRequest createInvalidExportTraceRequest() {
        final io.opentelemetry.proto.trace.v1.Span testSpan = Span.newBuilder()
                .setTraceState("SUCCESS").build();
        final ExportTraceServiceRequest successRequest = ExportTraceServiceRequest.newBuilder()
                .addResourceSpans(ResourceSpans.newBuilder()
                        .addScopeSpans(ScopeSpans.newBuilder().addSpans(testSpan)).build())
                .build();

        return successRequest;
    }

    private ExportTraceServiceRequest createExportTraceRequest() {
        final io.opentelemetry.proto.trace.v1.Span testSpan = Span.newBuilder()
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
        // assertThat("Http Status", response.status(), equalTo(expectedStatus));
        // assertThat("Http Response Throwable", throwable, is(nullValue()));

        final List<String> headerKeys = response.headers()
                .stream()
                .map(Map.Entry::getKey)
                .map(AsciiString::toString)
                .collect(Collectors.toList());
        // assertThat("Response Header Keys", headerKeys, not(contains("server")));
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
