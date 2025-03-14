/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.otelmetrics;


import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import io.grpc.BindableService;
import io.grpc.ServerServiceDefinition;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Statistic;
import io.netty.util.AsciiString;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;

import io.opentelemetry.proto.metrics.v1.Gauge;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
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
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.GrpcBasicAuthenticationProvider;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.certificate.CertificateProvider;
import org.opensearch.dataprepper.plugins.certificate.model.Certificate;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.health.HealthGrpcService;
import org.opensearch.dataprepper.plugins.otel.codec.OTelMetricDecoder;
import org.opensearch.dataprepper.plugins.source.otelmetrics.certificate.CertificateProviderFactory;

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
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig.DEFAULT_PORT;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig.DEFAULT_REQUEST_TIMEOUT_MS;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfig.SSL;

@ExtendWith(MockitoExtension.class)
class OTelMetricsSourceTest {
    private static final String GRPC_ENDPOINT = "gproto+http://127.0.0.1:21891/";
    private static final String TEST_PIPELINE_NAME = "test_pipeline";
    private static final String USERNAME = "test_user";
    private static final String PASSWORD = "test_password";
    private static final String TEST_PATH = "${pipelineName}/v1/metrics";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ExportMetricsServiceRequest METRICS_REQUEST = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(ResourceMetrics.newBuilder().build()).build();

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
    private OTelMetricsSourceConfig oTelMetricsSourceConfig;

    @Mock
    private BlockingBuffer<Record<? extends Metric>> buffer;

    @Mock
    private HttpBasicAuthenticationConfig httpBasicAuthenticationConfig;

    private PluginSetting pluginSetting;
    private PluginSetting testPluginSetting;
    private PluginMetrics pluginMetrics;
    private PipelineDescription pipelineDescription;
    private OTelMetricsSource SOURCE;

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
        pluginMetrics = PluginMetrics.fromNames("otel_metrics", "pipeline");

        when(oTelMetricsSourceConfig.getPort()).thenReturn(DEFAULT_PORT);
        when(oTelMetricsSourceConfig.isSsl()).thenReturn(false);
        when(oTelMetricsSourceConfig.getRequestTimeoutInMillis()).thenReturn(DEFAULT_REQUEST_TIMEOUT_MS);
        when(oTelMetricsSourceConfig.getMaxConnectionCount()).thenReturn(10);
        when(oTelMetricsSourceConfig.getThreadCount()).thenReturn(5);
        when(oTelMetricsSourceConfig.getCompression()).thenReturn(CompressionOption.NONE);

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(authenticationProvider);
        pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        SOURCE = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        assertTrue(SOURCE.getDecoder() instanceof OTelMetricDecoder);
    }

    @AfterEach
    public void afterEach() {
        SOURCE.stop();
    }

    private void configureObjectUnderTest() {
        SOURCE = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        assertTrue(SOURCE.getDecoder() instanceof OTelMetricDecoder);
    }

    @Test
    void testHttpFullJsonWithNonUnframedRequests() throws InvalidProtocolBufferException {
        SOURCE.start(buffer);
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21891")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(METRICS_REQUEST).getBytes()))
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
        pluginSetting = new PluginSetting("otel_metrics", settingsMap);
        pluginSetting.setPipelineName("pipeline");

        oTelMetricsSourceConfig = OBJECT_MAPPER.convertValue(pluginSetting.getSettings(), OTelMetricsSourceConfig.class);
        SOURCE = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        SOURCE.start(buffer);

        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTPS)
                        .authority("127.0.0.1:21891")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(METRICS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();
    }

    @Test
    void testHttpFullBytesWithNonUnframedRequests() {
        SOURCE.start(buffer);
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21891")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.copyOf(METRICS_REQUEST.toByteArray()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.UNSUPPORTED_MEDIA_TYPE, throwable))
                .join();
    }

    @Test
    void testHttpFullJsonWithUnframedRequests() throws InvalidProtocolBufferException {
        when(oTelMetricsSourceConfig.enableUnframedRequests()).thenReturn(true);
        SOURCE.start(buffer);

        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority("127.0.0.1:21891")
                                .method(HttpMethod.POST)
                                .path("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.copyOf(JsonFormat.printer().print(METRICS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();
    }

    @Test
    void testHttpCompressionWithUnframedRequests() throws IOException {
        when(oTelMetricsSourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelMetricsSourceConfig.getCompression()).thenReturn(CompressionOption.GZIP);
        SOURCE.start(buffer);

        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority("127.0.0.1:21891")
                                .method(HttpMethod.POST)
                                .path("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
                                .contentType(MediaType.JSON_UTF_8)
                                .add(HttpHeaderNames.CONTENT_ENCODING, "gzip")
                                .build(),
                        createGZipCompressedPayload(JsonFormat.printer().print(METRICS_REQUEST)))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();
    }

    @Test
    void testHttpFullJsonWithCustomPathAndUnframedRequests() throws InvalidProtocolBufferException {
        when(oTelMetricsSourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelMetricsSourceConfig.getPath()).thenReturn(TEST_PATH);
        SOURCE.start(buffer);

        final String transformedPath = "/" + TEST_PIPELINE_NAME + "/v1/metrics";
        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority("127.0.0.1:21891")
                                .method(HttpMethod.POST)
                                .path(transformedPath)
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.copyOf(JsonFormat.printer().print(METRICS_REQUEST).getBytes()))
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
        when(oTelMetricsSourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                        "username", USERNAME,
                        "password", PASSWORD
                )));
        when(oTelMetricsSourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelMetricsSourceConfig.getPath()).thenReturn(TEST_PATH);

        configureObjectUnderTest();
        SOURCE.start(buffer);

        final String encodeToString = Base64.getEncoder()
                .encodeToString(String.format("%s:%s", USERNAME, PASSWORD).getBytes(StandardCharsets.UTF_8));

        final String transformedPath = "/" + TEST_PIPELINE_NAME + "/v1/metrics";

        WebClient.of().prepare()
                .post("http://127.0.0.1:21891" + transformedPath)
                .content(MediaType.JSON_UTF_8, JsonFormat.printer().print(createExportMetricsRequest()).getBytes())
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
        when(oTelMetricsSourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                        "username", USERNAME,
                        "password", PASSWORD
                )));
        when(oTelMetricsSourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelMetricsSourceConfig.getPath()).thenReturn(TEST_PATH);

        configureObjectUnderTest();
        SOURCE.start(buffer);

        final String transformedPath = "/" + TEST_PIPELINE_NAME + "/v1/metrics";

        WebClient.of().prepare()
                .post("http://127.0.0.1:21891" + transformedPath)
                .content(MediaType.JSON_UTF_8, JsonFormat.printer().print(createExportMetricsRequest()).getBytes())
                .execute()
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.UNAUTHORIZED, throwable))
                .join();
    }

    @Test
    void testHttpRequestWithInvalidCredentials_with_unsuccessful_response() throws InvalidProtocolBufferException {
        when(httpBasicAuthenticationConfig.getUsername()).thenReturn(USERNAME);
        when(httpBasicAuthenticationConfig.getPassword()).thenReturn(PASSWORD);
        final GrpcAuthenticationProvider grpcAuthenticationProvider = new GrpcBasicAuthenticationProvider(httpBasicAuthenticationConfig);
    
        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(grpcAuthenticationProvider);
        when(oTelMetricsSourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                        "username", USERNAME,
                        "password", PASSWORD
                )));
        when(oTelMetricsSourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelMetricsSourceConfig.getPath()).thenReturn(TEST_PATH);
    
        configureObjectUnderTest();
        SOURCE.start(buffer);
    
        final String invalidUsername = "wrong_user";
        final String invalidPassword = "wrong_password";
        final String invalidCredentials = Base64.getEncoder()
                .encodeToString(String.format("%s:%s", invalidUsername, invalidPassword).getBytes(StandardCharsets.UTF_8));
    
        final String transformedPath = "/" + TEST_PIPELINE_NAME + "/v1/metrics";
    
        WebClient.of().prepare()
                .post("http://127.0.0.1:21891" + transformedPath)
                .content(MediaType.JSON_UTF_8, JsonFormat.printer().print(createExportMetricsRequest()).getBytes())
                .header("Authorization", "Basic " + invalidCredentials)
                .execute()
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.UNAUTHORIZED, throwable))
                .join();
    }
    
    @Test
    void testGrpcRequestWithInvalidCredentials_with_unsuccessful_response() throws Exception {
        when(httpBasicAuthenticationConfig.getUsername()).thenReturn(USERNAME);
        when(httpBasicAuthenticationConfig.getPassword()).thenReturn(PASSWORD);
        final GrpcAuthenticationProvider grpcAuthenticationProvider = new GrpcBasicAuthenticationProvider(httpBasicAuthenticationConfig);

        when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(grpcAuthenticationProvider);
        when(oTelMetricsSourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                        "username", USERNAME,
                        "password", PASSWORD
                )));
        configureObjectUnderTest();
        SOURCE.start(buffer);

        final String invalidUsername = "wrong_user";
        final String invalidPassword = "wrong_password";
        final String invalidCredentials = Base64.getEncoder()
                .encodeToString(String.format("%s:%s", invalidUsername, invalidPassword).getBytes(StandardCharsets.UTF_8));

        final MetricsServiceGrpc.MetricsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .addHeader("Authorization", "Basic " + invalidCredentials)
                .build(MetricsServiceGrpc.MetricsServiceBlockingStub.class);

        final StatusRuntimeException actualException = assertThrows(StatusRuntimeException.class, () -> client.export(createExportMetricsRequest()));

        assertThat(actualException.getStatus(), notNullValue());
        assertThat(actualException.getStatus().getCode(), equalTo(Status.Code.UNAUTHENTICATED));
    }

    @Test
    void testHttpWithoutSslFailsWhenSslIsEnabled() throws InvalidProtocolBufferException {
        when(oTelMetricsSourceConfig.isSsl()).thenReturn(true);
        when(oTelMetricsSourceConfig.getSslKeyCertChainFile()).thenReturn("data/certificate/test_cert.crt");
        when(oTelMetricsSourceConfig.getSslKeyFile()).thenReturn("data/certificate/test_decrypted_key.key");
        configureObjectUnderTest();
        SOURCE.start(buffer);
    
        WebClient client = WebClient.builder("http://127.0.0.1:21891")
                .build();
    
        CompletionException exception = assertThrows(CompletionException.class, () -> client.execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21891")
                        .method(HttpMethod.POST)
                        .path("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.copyOf(JsonFormat.printer().print(createExportMetricsRequest()).getBytes()))
                .aggregate()
                .join());
    
        assertThat(exception.getCause(), instanceOf(ClosedSessionException.class));
    }
    
    @Test
    void testGrpcFailsIfSslIsEnabledAndNoTls() {
        when(oTelMetricsSourceConfig.isSsl()).thenReturn(true);
        when(oTelMetricsSourceConfig.getSslKeyCertChainFile()).thenReturn("data/certificate/test_cert.crt");
        when(oTelMetricsSourceConfig.getSslKeyFile()).thenReturn("data/certificate/test_decrypted_key.key");
        configureObjectUnderTest();
        SOURCE.start(buffer);
    
        MetricsServiceGrpc.MetricsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .build(MetricsServiceGrpc.MetricsServiceBlockingStub.class);
    
        StatusRuntimeException actualException = assertThrows(StatusRuntimeException.class, () -> client.export(createExportMetricsRequest()));
  
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
            oTelMetricsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelMetricsSourceConfig.class);
            final OTelMetricsSource source = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
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
            oTelMetricsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelMetricsSourceConfig.class);
            final OTelMetricsSource source = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
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

            oTelMetricsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelMetricsSourceConfig.class);
            final OTelMetricsSource source = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
            source.start(buffer);
            source.stop();
        }

        verify(grpcServiceBuilder, times(1)).useClientTimeoutHeader(false);
        verify(grpcServiceBuilder, times(1)).useBlockingTaskExecutor(true);
        verify(grpcServiceBuilder).addService(isA(HealthGrpcService.class));
        verify(serverBuilder, never()).service(eq("/health"),isA(HealthCheckService.class));
    }

    @Test
    void start_with_Health_configured_includes_HealthCheck_service_Unauthenticated_disabled() throws IOException {
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

            oTelMetricsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelMetricsSourceConfig.class);
            final OTelMetricsSource source = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
            source.start(buffer);
            source.stop();
        }

        verify(grpcServiceBuilder, times(1)).useClientTimeoutHeader(false);
        verify(grpcServiceBuilder, times(1)).useBlockingTaskExecutor(true);
        verify(grpcServiceBuilder).addService(isA(HealthGrpcService.class));
        verify(serverBuilder, never()).service(eq("/health"),isA(HealthCheckService.class));
    }

    @Nested
    class UnauthTests {
        final Map<String, Object> settingsMap = new HashMap<>();

        @BeforeEach
        void setup() {
            when(authenticationProvider.getHttpAuthenticationService()).thenCallRealMethod();

            settingsMap.clear();
            settingsMap.put(SSL, false);
            settingsMap.put("health_check_service", "true");
            settingsMap.put("unframed_requests", "true");
            settingsMap.put("proto_reflection_service", "true");
            settingsMap.put("authentication", new PluginModel("http_basic",
                    Map.of(
                            "username", "test",
                            "password", "test2"
                    )));
        }

        void createObjectUnderTest() {
            testPluginSetting = new PluginSetting(null, settingsMap);
            testPluginSetting.setPipelineName("pipeline");

            oTelMetricsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelMetricsSourceConfig.class);
            SOURCE = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
            assertTrue(SOURCE.getDecoder() instanceof OTelMetricDecoder);
        }

        @Test
        void testHealthCheckUnauthNotAllowed() {
            settingsMap.put("unauthenticated_health_check", "false");
            createObjectUnderTest();

            SOURCE.start(buffer);

            // When
            WebClient.of().execute(RequestHeaders.builder()
                            .scheme(SessionProtocol.HTTP)
                            .authority("127.0.0.1:21891")
                            .method(HttpMethod.GET)
                            .path("/health")
                            .build())
                    .aggregate()
                    .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.UNAUTHORIZED, throwable))
                    .join();
        }

        @Test
        void testHealthCheckUnauthAllowed() {
            settingsMap.put("unauthenticated_health_check", "true");
            createObjectUnderTest();

            SOURCE.start(buffer);

            // When
            WebClient.of().execute(RequestHeaders.builder()
                            .scheme(SessionProtocol.HTTP)
                            .authority("127.0.0.1:21891")
                            .method(HttpMethod.GET)
                            .path("/health")
                            .build())
                    .aggregate()
                    .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                    .join();
        }
    }

    @Test
    void start_with_Health_configured_unframed_requests_includes_HealthCheck_service() throws IOException {
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

            oTelMetricsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelMetricsSourceConfig.class);
            final OTelMetricsSource source = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
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
            oTelMetricsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelMetricsSourceConfig.class);
            final OTelMetricsSource source = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
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
            oTelMetricsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelMetricsSourceConfig.class);
            final OTelMetricsSource source = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, certificateProviderFactory, pipelineDescription);
            source.start(buffer);
            source.stop();
        }

        verify(grpcServiceBuilder, times(1)).useClientTimeoutHeader(false);
        verify(grpcServiceBuilder, times(1)).useBlockingTaskExecutor(true);
        verify(grpcServiceBuilder, never()).addService(isA(HealthGrpcService.class));
        verify(serverBuilder, never()).service(eq("/health"),isA(HealthCheckService.class));
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

        testPluginSetting = new PluginSetting(null, Collections.singletonMap(SSL, false));
        testPluginSetting.setPipelineName("pipeline");
        oTelMetricsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelMetricsSourceConfig.class);
        final OTelMetricsSource source = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        //Expect RuntimeException because when port is already in use, BindException is thrown which is not RuntimeException
        assertThrows(RuntimeException.class, () -> source.start(buffer));
    }

    @Test
    void testStartWithEmptyBuffer() {
        testPluginSetting = new PluginSetting(null, Collections.singletonMap(SSL, false));
        testPluginSetting.setPipelineName("pipeline");
        oTelMetricsSourceConfig = OBJECT_MAPPER.convertValue(testPluginSetting.getSettings(), OTelMetricsSourceConfig.class);
        final OTelMetricsSource source = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        assertThrows(IllegalStateException.class, () -> source.start(null));
    }

    @Test
    void testStartWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelMetricsSource source = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
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
        final OTelMetricsSource source = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
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
        when(server.stop()).thenReturn(completableFuture);

        try (final MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            SOURCE.start(buffer);
        }

        verify(serverBuilder).service(isA(GrpcService.class));
    }

    @Test
    void testStopWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final OTelMetricsSource source = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
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
        final OTelMetricsSource source = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
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
        final OTelMetricsSource source = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
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
        final OTelMetricsSource source = new OTelMetricsSource(oTelMetricsSourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
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
        SOURCE.start(buffer);

        final MetricsServiceGrpc.MetricsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .build(MetricsServiceGrpc.MetricsServiceBlockingStub.class);
        final ExportMetricsServiceResponse exportResponse = client.export(createExportMetricsRequest());
        assertThat(exportResponse, notNullValue());

        final ArgumentCaptor<Collection<Record<? extends Metric>>> bufferWriteArgumentCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(buffer).writeAll(bufferWriteArgumentCaptor.capture(), anyInt());

        final Collection<Record<? extends Metric>> actualBufferWrites = bufferWriteArgumentCaptor.getValue();
        assertThat(actualBufferWrites, notNullValue());
        assertThat(actualBufferWrites.size(), equalTo(1));
    }

    @Test
    void gRPC_with_auth_request_writes_to_buffer_with_successful_response() throws Exception {
        when(httpBasicAuthenticationConfig.getUsername()).thenReturn(USERNAME);
        when(httpBasicAuthenticationConfig.getPassword()).thenReturn(PASSWORD);
        final GrpcAuthenticationProvider grpcAuthenticationProvider = new GrpcBasicAuthenticationProvider(httpBasicAuthenticationConfig);

        lenient().when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(grpcAuthenticationProvider);
        when(oTelMetricsSourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelMetricsSourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                        "username", USERNAME,
                        "password", PASSWORD
                )));
        configureObjectUnderTest();
        SOURCE.start(buffer);

        final String encodeToString = Base64.getEncoder()
                .encodeToString(String.format("%s:%s", USERNAME, PASSWORD).getBytes(StandardCharsets.UTF_8));

        final MetricsServiceGrpc.MetricsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .addHeader("Authorization", "Basic " + encodeToString)
                .build(MetricsServiceGrpc.MetricsServiceBlockingStub.class);
        final ExportMetricsServiceResponse exportResponse = client.export(createExportMetricsRequest());
        assertThat(exportResponse, notNullValue());

        final ArgumentCaptor<Collection<Record<? extends Metric>>> bufferWriteArgumentCaptor = ArgumentCaptor.forClass(Collection.class);
        verify(buffer).writeAll(bufferWriteArgumentCaptor.capture(), anyInt());

        final Collection<Record<? extends Metric>> actualBufferWrites = bufferWriteArgumentCaptor.getValue();
        assertThat(actualBufferWrites, notNullValue());
        assertThat(actualBufferWrites.size(), equalTo(1));
    }

    @Test
    void gRPC_request_with_custom_path_throws_when_written_to_default_path() {
        when(oTelMetricsSourceConfig.getPath()).thenReturn(TEST_PATH);
        when(oTelMetricsSourceConfig.enableUnframedRequests()).thenReturn(true);

        configureObjectUnderTest();
        SOURCE.start(buffer);

        final MetricsServiceGrpc.MetricsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .build(MetricsServiceGrpc.MetricsServiceBlockingStub.class);

        final StatusRuntimeException actualException = assertThrows(StatusRuntimeException.class, () -> client.export(createExportMetricsRequest()));
        assertThat(actualException.getStatus(), notNullValue());
        assertThat(actualException.getStatus().getCode(), equalTo(Status.UNIMPLEMENTED.getCode()));
    }

    @ParameterizedTest
    @ArgumentsSource(BufferExceptionToStatusArgumentsProvider.class)
    void gRPC_request_returns_expected_status_for_exceptions_from_buffer(
            final Class<Exception> bufferExceptionClass,
            final Status.Code expectedStatusCode) throws Exception {
        SOURCE.start(buffer);

        final MetricsServiceGrpc.MetricsServiceBlockingStub client = Clients.builder(GRPC_ENDPOINT)
                .build(MetricsServiceGrpc.MetricsServiceBlockingStub.class);

        doThrow(bufferExceptionClass)
                .when(buffer)
                .writeAll(any(Collection.class), anyInt());
        final ExportMetricsServiceRequest exportMetricsRequest = createExportMetricsRequest();
        final StatusRuntimeException actualException = assertThrows(StatusRuntimeException.class, () -> client.export(exportMetricsRequest));

        assertThat(actualException.getStatus(), notNullValue());
        assertThat(actualException.getStatus().getCode(), equalTo(expectedStatusCode));
    }

    @Test
    void request_that_exceeds_maxRequestLength_returns_413() throws InvalidProtocolBufferException {
        when(oTelMetricsSourceConfig.enableUnframedRequests()).thenReturn(true);
        when(oTelMetricsSourceConfig.getMaxRequestLength()).thenReturn(ByteCount.ofBytes(4));
        SOURCE.start(buffer);

        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority("127.0.0.1:21891")
                                .method(HttpMethod.POST)
                                .path("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.copyOf(JsonFormat.printer().print(METRICS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.REQUEST_ENTITY_TOO_LARGE, throwable))
                .join();
    }

    @Test
    void testServerConnectionsMetric() throws InvalidProtocolBufferException {
        // Prepare
        when(oTelMetricsSourceConfig.enableUnframedRequests()).thenReturn(true);
        SOURCE.start(buffer);

        final String metricNamePrefix = new StringJoiner(MetricNames.DELIMITER)
                .add("pipeline").add("otel_metrics").toString();
        List<Measurement> serverConnectionsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(OTelMetricsSource.SERVER_CONNECTIONS).toString());

        // Verify connections metric value is 0
        Measurement serverConnectionsMeasurement = MetricsTestUtil.getMeasurementFromList(serverConnectionsMeasurements, Statistic.VALUE);
        assertEquals(0, serverConnectionsMeasurement.getValue());

        final RequestHeaders testRequestHeaders = RequestHeaders.builder()
                .scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:21891")
                .method(HttpMethod.POST)
                .path("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export")
                .contentType(MediaType.JSON_UTF_8)
                .build();
        final HttpData testHttpData = HttpData.copyOf(JsonFormat.printer().print(METRICS_REQUEST).getBytes());

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
                    arguments(RuntimeException.class, Status.Code.INTERNAL)
            );
        }
    }

    private ExportMetricsServiceRequest createExportMetricsRequest() {
        final Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue("service").build())
                ).build();
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
