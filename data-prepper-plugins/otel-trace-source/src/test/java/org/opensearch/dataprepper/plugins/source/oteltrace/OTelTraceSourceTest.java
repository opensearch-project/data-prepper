/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.oteltrace;

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
import com.linecorp.armeria.server.HttpService;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.grpc.GrpcService;
import com.linecorp.armeria.server.grpc.GrpcServiceBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Statistic;
import io.netty.util.AsciiString;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.collector.trace.v1.TraceServiceGrpc;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Span;
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
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.metrics.MetricNames;
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
import org.opensearch.dataprepper.plugins.health.HealthGrpcService;
import org.opensearch.dataprepper.plugins.source.oteltrace.certificate.CertificateProviderFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

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
import static org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_PORT;
import static org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;
import static org.opensearch.dataprepper.plugins.source.oteltrace.OTelTraceSourceConfig.SSL;

@ExtendWith(MockitoExtension.class)
class OTelTraceSourceTest {
    private static final String GRPC_ENDPOINT = "gproto+http://127.0.0.1:21890/";
    private static final String USERNAME = "test_user";
    private static final String PASSWORD = "test_password";
    private static final String TEST_PATH = "${pipelineName}/v1/traces";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().registerModule(new JavaTimeModule());
    private static final String TEST_PIPELINE_NAME = "test_pipeline";
    private static final RetryInfoConfig TEST_RETRY_INFO = new RetryInfoConfig(Duration.ofMillis(50), Duration.ofMillis(2000));
    private static final ExportTraceServiceRequest SUCCESS_REQUEST = ExportTraceServiceRequest.newBuilder()
            .addResourceSpans(ResourceSpans.newBuilder()
                    .addScopeSpans(ScopeSpans.newBuilder()
                            .addSpans(io.opentelemetry.proto.trace.v1.Span.newBuilder().setTraceState("SUCCESS").build())).build()).build();
    private static final ExportTraceServiceRequest FAILURE_REQUEST = ExportTraceServiceRequest.newBuilder()
            .addResourceSpans(ResourceSpans.newBuilder()
                    .addScopeSpans(ScopeSpans.newBuilder()
                            .addSpans(io.opentelemetry.proto.trace.v1.Span.newBuilder().setTraceState("FAILURE").build())).build()).build();

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

    private PluginSetting pluginSetting;
    private PluginSetting testPluginSetting;
    private PluginMetrics pluginMetrics;
    private PipelineDescription pipelineDescription;
    private OTelTraceSource SOURCE;

    @BeforeEach
    void beforeEach() {
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

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
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
    void testHttpFullJsonWithNonUnframedRequests() throws InvalidProtocolBufferException {
        configureObjectUnderTest();
        SOURCE.start(buffer);
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(SUCCESS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(FAILURE_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
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
        pluginSetting = new PluginSetting("otel_trace", settingsMap);
        pluginSetting.setPipelineName("pipeline");

        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelTraceSourceConfig.class);
        SOURCE = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        SOURCE.start(buffer);

        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTPS)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(SUCCESS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();
        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTPS)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(FAILURE_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();
    }

    @Test
    void testHttpFullBytesWithNonUnframedRequests() {
        configureObjectUnderTest();
        SOURCE.start(buffer);
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.copyOf(SUCCESS_REQUEST.toByteArray()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.copyOf(FAILURE_REQUEST.toByteArray()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();
    }

    @Test
    void testHttpFullJsonWithUnframedRequests() throws InvalidProtocolBufferException {
        when(oTelTraceSourceConfig.enableUnframedRequests()).thenReturn(true);
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
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();
    }

    @Test
    void testHttpCompressionWithUnframedRequests() throws IOException {
        when(oTelTraceSourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelTraceSourceConfig.getCompression()).thenReturn(CompressionOption.GZIP);
        configureObjectUnderTest();
        SOURCE.start(buffer);

        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority("127.0.0.1:21890")
                                .method(HttpMethod.POST)
                                .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                                .contentType(MediaType.JSON_UTF_8)
                                .add(HttpHeaderNames.CONTENT_ENCODING, "gzip")
                                .build(),
                        createGZipCompressedPayload(JsonFormat.printer().print(createExportTraceRequest())))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();
    }

    @Test
    void testHttpFullJsonWithCustomPathAndUnframedRequests() throws InvalidProtocolBufferException {
        when(oTelTraceSourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelTraceSourceConfig.getPath()).thenReturn(TEST_PATH);
        configureObjectUnderTest();
        SOURCE.start(buffer);

        final String transformedPath = "/" + TEST_PIPELINE_NAME + "/v1/traces";
        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority("127.0.0.1:21890")
                                .method(HttpMethod.POST)
                                .path(transformedPath)
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.copyOf(JsonFormat.printer().print(createExportTraceRequest()).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();
    }

    @Test
    void testHttpFullJsonWithCustomPathAndAuthHeader_with_successful_response() throws InvalidProtocolBufferException {
        when(httpBasicAuthenticationConfig.getUsername()).thenReturn(USERNAME);
        when(httpBasicAuthenticationConfig.getPassword()).thenReturn(PASSWORD);
        final GrpcAuthenticationProvider grpcAuthenticationProvider = new GrpcBasicAuthenticationProvider(httpBasicAuthenticationConfig);

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(grpcAuthenticationProvider);
        when(oTelTraceSourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                        "username", USERNAME,
                        "password", PASSWORD
                )));
        when(oTelTraceSourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelTraceSourceConfig.getPath()).thenReturn(TEST_PATH);

        configureObjectUnderTest();
        SOURCE.start(buffer);

        final String encodeToString = Base64.getEncoder()
                .encodeToString(String.format("%s:%s", USERNAME, PASSWORD).getBytes(StandardCharsets.UTF_8));

        final String transformedPath = "/" + TEST_PIPELINE_NAME + "/v1/traces";

        WebClient.of().prepare()
                .post("http://127.0.0.1:21890" + transformedPath)
                .content(MediaType.JSON_UTF_8, JsonFormat.printer().print(createExportTraceRequest()).getBytes())
                .header("Authorization", "Basic " + encodeToString)
                .execute()
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();
    }

    @Test
    void testHttpFullJsonWithCustomPathAndAuthHeader_with_unsuccessful_response() throws InvalidProtocolBufferException {
        when(httpBasicAuthenticationConfig.getUsername()).thenReturn(USERNAME);
        when(httpBasicAuthenticationConfig.getPassword()).thenReturn(PASSWORD);
        final GrpcAuthenticationProvider grpcAuthenticationProvider = new GrpcBasicAuthenticationProvider(httpBasicAuthenticationConfig);

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(grpcAuthenticationProvider);
        when(oTelTraceSourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                        "username", USERNAME,
                        "password", PASSWORD
                )));
        when(oTelTraceSourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelTraceSourceConfig.getPath()).thenReturn(TEST_PATH);

        configureObjectUnderTest();
        SOURCE.start(buffer);

        final String transformedPath = "/" + TEST_PIPELINE_NAME + "/v1/traces";

        WebClient.of().prepare()
                .post("http://127.0.0.1:21890" + transformedPath)
                .content(MediaType.JSON_UTF_8, JsonFormat.printer().print(createExportTraceRequest()).getBytes())
                .execute()
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.UNAUTHORIZED, throwable))
                .join();
    }

    @Test
    void testHttpRequestWithInvalidCredentialsShouldReturnUnauthorized() throws InvalidProtocolBufferException {
        when(httpBasicAuthenticationConfig.getUsername()).thenReturn(USERNAME);
        when(httpBasicAuthenticationConfig.getPassword()).thenReturn(PASSWORD);
        final GrpcAuthenticationProvider grpcAuthenticationProvider = new GrpcBasicAuthenticationProvider(httpBasicAuthenticationConfig);

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(grpcAuthenticationProvider);
        when(oTelTraceSourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                        "username", USERNAME,
                        "password", PASSWORD
                )));
        when(oTelTraceSourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelTraceSourceConfig.getPath()).thenReturn(TEST_PATH);
        configureObjectUnderTest();
        SOURCE.start(buffer);
    
        final String transformedPath = "/" + TEST_PIPELINE_NAME + "/v1/traces";
    
        final String invalidUsername = "wrong_user";
        final String invalidPassword = "wrong_password";
        final String invalidCredentials = Base64.getEncoder()
                .encodeToString(String.format("%s:%s", invalidUsername, invalidPassword).getBytes(StandardCharsets.UTF_8));
    
        WebClient.of().prepare()
                .post("http://127.0.0.1:21890" + transformedPath)
                .content(MediaType.JSON_UTF_8, JsonFormat.printer().print(createExportTraceRequest()).getBytes())
                .header("Authorization", "Basic " + invalidCredentials)
                .execute()
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.UNAUTHORIZED, throwable))
                .join();
    }

    @Test
    void testGrpcRequestWithoutAuthentication_with_unsuccessful_response() throws Exception {
        when(httpBasicAuthenticationConfig.getUsername()).thenReturn(USERNAME);
        when(httpBasicAuthenticationConfig.getPassword()).thenReturn(PASSWORD);
        final GrpcAuthenticationProvider grpcAuthenticationProvider = new GrpcBasicAuthenticationProvider(httpBasicAuthenticationConfig);
    
        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(grpcAuthenticationProvider);
        when(oTelTraceSourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                        "username", USERNAME,
                        "password", PASSWORD
                )));
        configureObjectUnderTest();
        SOURCE.start(buffer);
    
        final TraceServiceGrpc.TraceServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .build(TraceServiceGrpc.TraceServiceBlockingStub.class);
    
        final StatusRuntimeException actualException = assertThrows(StatusRuntimeException.class, () -> client.export(createExportTraceRequest()));
    
        assertThat(actualException.getStatus(), notNullValue());
        assertThat(actualException.getStatus().getCode(), equalTo(Status.Code.UNAUTHENTICATED));
    }

    @Test
    void testHttpWithoutSslFailsWhenSslIsEnabled() throws InvalidProtocolBufferException {
        when(oTelTraceSourceConfig.isSsl()).thenReturn(true);
        when(oTelTraceSourceConfig.getSslKeyCertChainFile()).thenReturn("data/certificate/test_cert.crt");
        when(oTelTraceSourceConfig.getSslKeyFile()).thenReturn("data/certificate/test_decrypted_key.key");
        configureObjectUnderTest();
        SOURCE.start(buffer);
    
        WebClient client = WebClient.builder("http://127.0.0.1:21890")
                .build();
    
        CompletionException exception = assertThrows(CompletionException.class, () -> client.execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21890")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(createExportTraceRequest()).getBytes()))
                .aggregate()
                .join());
    
        assertThat(exception.getCause(), instanceOf(ClosedSessionException.class));
    }
    
    @Test
    void testGrpcFailsIfSslIsEnabledAndNoTls() {
        when(oTelTraceSourceConfig.isSsl()).thenReturn(true);
        when(oTelTraceSourceConfig.getSslKeyCertChainFile()).thenReturn("data/certificate/test_cert.crt");
        when(oTelTraceSourceConfig.getSslKeyFile()).thenReturn("data/certificate/test_decrypted_key.key");
        configureObjectUnderTest();
        SOURCE.start(buffer);
    
        TraceServiceGrpc.TraceServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .build(TraceServiceGrpc.TraceServiceBlockingStub.class);
    
        StatusRuntimeException actualException = assertThrows(StatusRuntimeException.class, () -> client.export(createExportTraceRequest()));
    
        assertThat(actualException.getStatus(), notNullValue());
        assertThat(actualException.getStatus().getCode(), equalTo(Status.Code.UNKNOWN));
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

            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");
            oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
            final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
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
            final Map<String, Object> settingsMap = new HashMap<>();
            settingsMap.put(SSL, true);
            settingsMap.put("useAcmCertForSSL", true);
            settingsMap.put("awsRegion", "us-east-1");
            settingsMap.put("acmCertificateArn", "arn:aws:acm:us-east-1:account:certificate/1234-567-856456");
            settingsMap.put("sslKeyCertChainFile", "data/certificate/test_cert.crt");
            settingsMap.put("sslKeyFile", "data/certificate/test_decrypted_key.key");

            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");
            oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
            final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
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

            oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
            final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
            source.start(buffer);
            source.stop();
        }

        verify(grpcServiceBuilder, times(1)).useClientTimeoutHeader(false);
        verify(grpcServiceBuilder, times(1)).useBlockingTaskExecutor(true);
        verify(grpcServiceBuilder).addService(isA(HealthGrpcService.class));
        verify(serverBuilder, never()).service(eq("/health"),isA(HealthCheckService.class));
    }

    @Test
    void start_with_Health_configured_unframed_requests_includes_HTTPHealthCheck_service() throws IOException {
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
            settingsMap.put("unframed_requests", "true");

            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");

            oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
            final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
            source.start(buffer);
            source.stop();
        }

        verify(grpcServiceBuilder, times(1)).useClientTimeoutHeader(false);
        verify(grpcServiceBuilder, times(1)).useBlockingTaskExecutor(true);
        verify(grpcServiceBuilder).addService(isA(HealthGrpcService.class));
        verify(serverBuilder).service(eq("/health"), isA(HealthCheckService.class));
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
            oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
            final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
            source.start(buffer);
            source.stop();
        }

        verify(grpcServiceBuilder, times(1)).useClientTimeoutHeader(false);
        verify(grpcServiceBuilder, times(1)).useBlockingTaskExecutor(true);
        verify(grpcServiceBuilder, never()).addService(isA(HealthGrpcService.class));
        verify(serverBuilder, never()).service(eq("/health"),isA(HealthCheckService.class));
    }

    @Test
    void start_without_Health_configured_unframed_requests_does_not_include_HealthCheck_service() throws IOException {
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
            settingsMap.put("unframed_requests", "true");

            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");
            oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
            final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
            source.start(buffer);
            source.stop();
        }

        verify(grpcServiceBuilder, times(1)).useClientTimeoutHeader(false);
        verify(grpcServiceBuilder, times(1)).useBlockingTaskExecutor(true);
        verify(grpcServiceBuilder, never()).addService(isA(HealthGrpcService.class));
        verify(serverBuilder, never()).service(eq("/health"),isA(HealthCheckService.class));
    }

    @Test
    void testHealthCheckUnauthNotAllowed() {
        // Prepare
        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put(SSL, false);
        settingsMap.put("health_check_service", "true");
        settingsMap.put("unframed_requests", "true");
        settingsMap.put("proto_reflection_service", "true");
        settingsMap.put("unauthenticated_health_check", "false");
        settingsMap.put("authentication", new PluginModel("http_basic",
                Map.of(
                        "username", "test",
                        "password", "test2"
                )));

        testPluginSetting = new PluginSetting(null, settingsMap);
        testPluginSetting.setPipelineName("pipeline");

        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);

        source.start(buffer);

        // When
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("localhost:21890")
                        .method(HttpMethod.GET)
                        .path("/health")
                        .build())
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.UNAUTHORIZED, throwable))
                .join();

        source.stop();
    }

    @Test
    void testHealthCheckUnauthAllowed() {
        // Prepare
        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put(SSL, false);
        settingsMap.put("health_check_service", "true");
        settingsMap.put("unframed_requests", "true");
        settingsMap.put("proto_reflection_service", "true");
        settingsMap.put("unauthenticated_health_check", "true");
        settingsMap.put("authentication", new PluginModel("http_basic",
                Map.of(
                        "username", "test",
                        "password", "test2"
                )));

        testPluginSetting = new PluginSetting(null, settingsMap);
        testPluginSetting.setPipelineName("pipeline");

        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);

        source.start(buffer);

        // When
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("localhost:21890")
                        .method(HttpMethod.GET)
                        .path("/health")
                        .build())
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable)).join();

        source.stop();
    }

    @Test
    void testOptionalHttpAuthServiceNotInPlace() {
        when(server.stop()).thenReturn(completableFuture);

        try (final MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            SOURCE.start(buffer);
        }

        verify(serverBuilder).service(isA(GrpcService.class));
        verify(serverBuilder, never()).decorator(isA(Function.class));
    }

    @Test
    void testOptionalHttpAuthServiceInPlace() {
        final Optional<Function<? super HttpService, ? extends HttpService>> function = Optional.of(httpService -> httpService);

        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("authentication", new PluginModel("test", null));
        settingsMap.put("unauthenticated_health_check", true);

        settingsMap.put(SSL, false);

        testPluginSetting = new PluginSetting(null, settingsMap);
        testPluginSetting.setPipelineName("pipeline");
        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);

        when(authenticationProvider.getHttpAuthenticationService()).thenReturn(function);

        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);

        try (final MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(buffer);
        }

        verify(serverBuilder).service(isA(GrpcService.class));
        verify(serverBuilder).decorator(isA(String.class), isA(Function.class));
    }

    @Test
    void testOptionalHttpAuthServiceInPlaceWithUnauthenticatedDisabled() {
        final Optional<Function<? super HttpService, ? extends HttpService>> function = Optional.of(httpService -> httpService);

        final Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("authentication", new PluginModel("test", null));
        settingsMap.put("unauthenticated_health_check", false);

        settingsMap.put(SSL, false);

        testPluginSetting = new PluginSetting(null, settingsMap);
        testPluginSetting.setPipelineName("pipeline");
        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);

        when(authenticationProvider.getHttpAuthenticationService()).thenReturn(function);

        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);

        try (final MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(buffer);
        }

        verify(serverBuilder).service(isA(GrpcService.class));
        verify(serverBuilder).decorator(isA(Function.class));
    }

    @Test
    void testDoubleStart() {
        // starting server
        SOURCE.start(buffer);
        // double start server
        assertThrows(IllegalStateException.class, () -> SOURCE.start(buffer));
    }

    @Test
    void testRunAnotherSourceWithSamePort() {
        // starting server
        SOURCE.start(buffer);


        Map<String, Object> settingsMap = Map.of("retry_info", TEST_RETRY_INFO, SSL, false);
        testPluginSetting = new PluginSetting(null, settingsMap);
        testPluginSetting.setPipelineName("pipeline");
        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        //Expect RuntimeException because when port is already in use, BindException is thrown which is not RuntimeException
        assertThrows(RuntimeException.class, () -> source.start(buffer));
    }

    @Test
    void testStartWithEmptyBuffer() {
        testPluginSetting = new PluginSetting(null, Collections.singletonMap(SSL, false));
        testPluginSetting.setPipelineName("pipeline");
        oTelTraceSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelTraceSourceConfig.class);
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        assertThrows(IllegalStateException.class, () -> source.start(null));
    }

    @Test
    void testStartWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
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
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
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
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
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
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
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
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
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
        final OTelTraceSource source = new OTelTraceSource(oTelTraceSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
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
        assertThat(actualException.getStatus().getCode(), equalTo(Status.UNIMPLEMENTED.getCode()));
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
        assertThat(actualException.getStatus().getCode(), equalTo(expectedStatusCode));
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
        assertThat(actualException.getStatus().getCode(), equalTo(Status.Code.INVALID_ARGUMENT));

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

    @Test
    void testServerConnectionsMetric() throws InvalidProtocolBufferException {
        // Prepare
        when(oTelTraceSourceConfig.enableUnframedRequests()).thenReturn(true);
        SOURCE.start(buffer);

        final String metricNamePrefix = new StringJoiner(MetricNames.DELIMITER)
                .add("pipeline").add("otel_trace").toString();
        List<Measurement> serverConnectionsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(OTelTraceSource.SERVER_CONNECTIONS).toString());

        // Verify connections metric value is 0
        Measurement serverConnectionsMeasurement = MetricsTestUtil.getMeasurementFromList(serverConnectionsMeasurements, Statistic.VALUE);
        assertEquals(0, serverConnectionsMeasurement.getValue());

        final RequestHeaders testRequestHeaders = RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21890")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.trace.v1.TraceService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .build();
        final HttpData testHttpData = HttpData.copyOf(JsonFormat.printer().print(createExportTraceRequest()).getBytes());

        // Send request
        WebClient.of().execute(testRequestHeaders, testHttpData)
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();

        // Verify connections metric value is 1
        serverConnectionsMeasurement = MetricsTestUtil.getMeasurementFromList(serverConnectionsMeasurements, Statistic.VALUE);
        assertEquals(1.0, serverConnectionsMeasurement.getValue());
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
        assertThat("Http Status", response.status(), equalTo(expectedStatus));
        assertThat("Http Response Throwable", throwable, is(nullValue()));

        final List<String> headerKeys = response.headers()
                .stream()
                .map(Map.Entry::getKey)
                .map(AsciiString::toString)
                .collect(Collectors.toList());
        assertThat("Response Header Keys", headerKeys, not(contains("server")));
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
