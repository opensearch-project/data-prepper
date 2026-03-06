/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.otelmetrics;

import static com.linecorp.armeria.common.HttpStatus.INSUFFICIENT_STORAGE;
import static com.linecorp.armeria.common.HttpStatus.INTERNAL_SERVER_ERROR;
import static com.linecorp.armeria.common.HttpStatus.REQUEST_ENTITY_TOO_LARGE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
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
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfigFixture.createBuilderForConfigWithSsl;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfigFixture.createConfigBuilderWithBasicAuth;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfigFixture.createDefaultConfig;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfigFixture.createDefaultConfigBuilder;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfigFixture.createJsonHttpPayload;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OTelMetricsSourceConfigFixture.createMetricsServiceRequest;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OtelMetricsSourceConfigTestData.BASIC_AUTH_PASSWORD;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OtelMetricsSourceConfigTestData.BASIC_AUTH_USERNAME;
import static org.opensearch.dataprepper.plugins.source.otelmetrics.OtelMetricsSourceConfigTestData.CONFIG_HTTP_PATH;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.metrics.v1.ScopeMetrics;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.verification.VerificationMode;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.GrpcAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.GrpcBasicAuthenticationProvider;
import org.opensearch.dataprepper.plugins.HttpBasicArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.otel.codec.OTelMetricDecoder;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.linecorp.armeria.client.ClientFactory;
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
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Statistic;
import io.netty.util.AsciiString;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.resource.v1.Resource;


@ExtendWith(MockitoExtension.class)
class OTelMetricsSourceHttpTest {
    private static final String TEST_PIPELINE_NAME = "test_pipeline";

    @Mock
    private ServerBuilder serverBuilder;

    @Mock
    private Server server;

    @Mock
    private GrpcServiceBuilder grpcServiceBuilder;

    @Mock
    private GrpcService grpcService;

    @Mock
    private PluginFactory pluginFactory;

    @Mock
    private GrpcBasicAuthenticationProvider authenticationProvider;

    @Mock
    private BlockingBuffer<Record<? extends Metric>> buffer;

    private PluginMetrics pluginMetrics;
    private PipelineDescription pipelineDescription;
    private OTelMetricsSource SOURCE;
    private static final ExportMetricsServiceRequest METRICS_REQUEST = ExportMetricsServiceRequest.newBuilder()
            .addResourceMetrics(ResourceMetrics.newBuilder().build()).build();

    @BeforeEach
    public void beforeEach() {
        lenient().when(serverBuilder.service(any(GrpcService.class))).thenReturn(serverBuilder);
        lenient().when(serverBuilder.https(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);

        lenient().when(grpcServiceBuilder.addService(any(BindableService.class))).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useClientTimeoutHeader(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.useBlockingTaskExecutor(anyBoolean())).thenReturn(grpcServiceBuilder);
        lenient().when(grpcServiceBuilder.build()).thenReturn(grpcService);

        MetricsTestUtil.initMetrics();
        pluginMetrics = PluginMetrics.fromNames("otel_metrics", "pipeline");

        lenient().when(pluginFactory.loadPlugin(eq(GrpcAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(authenticationProvider);
        pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
    }

    @AfterEach
    public void afterEach() {
        SOURCE.stop();
    }

    private void configureSource() {
        configureSource(createDefaultConfig());
    }

    private void configureSource(OTelMetricsSourceConfig config) {
        SOURCE = new OTelMetricsSource(config, pluginMetrics, pluginFactory, pipelineDescription);
        assertInstanceOf(OTelMetricDecoder.class, SOURCE.getDecoder());
    }

    private RequestHeadersBuilder getDefaultRequestHeadersBuilder() {
        return RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:21891")
                        .method(HttpMethod.POST)
                        .path(CONFIG_HTTP_PATH)
                        .contentType(MediaType.JSON_UTF_8);
    }

    @ParameterizedTest
    @MethodSource("getPathParams")
    void httpRequest_writesToBuffer_returnsSuccessfulResponse(String givenPath, String resolvedRequestPath) throws Exception {
        OTelMetricsSourceConfig config = createDefaultConfigBuilder().httpPath(givenPath).build();
        configureSource(config);
        SOURCE.start(buffer);
        ExportMetricsServiceRequest request = createMetricsServiceRequest();

        WebClient.of().execute(
                getDefaultRequestHeadersBuilder().path(resolvedRequestPath).scheme(SessionProtocol.HTTP).build(),
                HttpData.copyOf(JsonFormat.printer().print(request).getBytes())
        )
            .aggregate()
            .whenComplete((response, throwable) -> assertThat(response.status(), is(HttpStatus.OK)))
            .join();

        verify(buffer).writeAll(any(), anyInt());
    }

    @Test
    void httpRequest_payloadIsProtobuf_returnsSuccessfulResponse() throws Exception {
        configureSource();
        SOURCE.start(buffer);
        ExportMetricsServiceRequest request = createMetricsServiceRequest();

        WebClient.of().execute(
                        getDefaultRequestHeadersBuilder().contentType(MediaType.PROTOBUF).scheme(SessionProtocol.HTTP).build(),
                        HttpData.copyOf(request.toByteArray())
                )
                .aggregate()
                .whenComplete((response, throwable) -> assertThat(response.status(), is(HttpStatus.OK)))
                .join();

        verify(buffer).writeAll(any(), anyInt());
    }

    @Test
    void httpsRequest_requestIsProcessed_writesToBufferAndReturnsSuccessfulResponse() throws Exception {
        configureSource(createBuilderForConfigWithSsl().build());
        SOURCE.start(buffer);
        ExportMetricsServiceRequest request = createExportMetricsRequest();

        WebClient.builder()
                .factory(ClientFactory.insecure()).
                build().execute(
                        getDefaultRequestHeadersBuilder().scheme(SessionProtocol.HTTPS).build(),
                        HttpData.copyOf(JsonFormat.printer().print(request).getBytes())
                )
                .aggregate()
                .whenComplete((response, throwable) -> assertThat(response.status(), is(HttpStatus.OK)))
                .join();

        verify(buffer).writeAll(any(), anyInt());
    }


    @Test
    void httpRequest_oneConnectionIsEstablished_metricsReflectCorrectConnectionCount() throws InvalidProtocolBufferException {
        configureSource();
        SOURCE.start(buffer);

        WebClient.of().execute(getDefaultRequestHeadersBuilder().build(), createJsonHttpPayload())
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();

        List<Measurement> serverConnectionsMeasurements = MetricsTestUtil.getMeasurementList("pipeline.otel_metrics.serverConnections");
        Measurement serverConnectionsMeasurement = MetricsTestUtil.getMeasurementFromList(serverConnectionsMeasurements, Statistic.VALUE);
        assertEquals(1.0, serverConnectionsMeasurement.getValue());

        SOURCE.stop();
    }

    @Test
    void httpRequest_payloadIsCompressed_returns200() throws IOException {
        configureSource( createDefaultConfigBuilder().compression(CompressionOption.GZIP).build());
        SOURCE.start(buffer);

        WebClient.of().execute(getDefaultRequestHeadersBuilder()
                                .add(HttpHeaderNames.CONTENT_ENCODING, "gzip")
                                .build(),
                        createGZipCompressedPayload(JsonFormat.printer().print(createMetricsServiceRequest())))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, HttpStatus.OK, throwable))
                .join();
    }

    @ParameterizedTest
    @MethodSource("getBasicAuthTestData")
    void httpRequest_withBasicAuth_returnsAppropriateResponse(String givenUsername, String givenPassword, HttpStatus expectedStatus, VerificationMode expectedBufferWrites) throws Exception {
        final HttpBasicAuthenticationConfig basicAuthConfig = new HttpBasicAuthenticationConfig(BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD);
        final HttpBasicArmeriaHttpAuthenticationProvider authProvider = new HttpBasicArmeriaHttpAuthenticationProvider(basicAuthConfig);
        when(pluginFactory.loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class))).thenReturn(authProvider);
        configureSource(createConfigBuilderWithBasicAuth().build());
        SOURCE.start(buffer);

        final String encodedCredentials = Base64.getEncoder().encodeToString(String.format("%s:%s", givenUsername, givenPassword).getBytes(StandardCharsets.UTF_8));
        WebClient.of().execute(getDefaultRequestHeadersBuilder()
                                .add(HttpHeaderNames.AUTHORIZATION, "Basic " + encodedCredentials)
                                .build(),
                        HttpData.copyOf(JsonFormat.printer().print(METRICS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, expectedStatus, throwable))
                .join();

        verify(buffer, expectedBufferWrites).writeAll(any(), anyInt());
    }

    private static Stream<Arguments> getBasicAuthTestData() {
        return Stream.of(
                Arguments.of(BASIC_AUTH_USERNAME, BASIC_AUTH_PASSWORD, HttpStatus.OK, times(1)),
                Arguments.of(BASIC_AUTH_USERNAME, "wrong password", HttpStatus.UNAUTHORIZED, times(0))
        );
    }

    @ParameterizedTest
    @MethodSource("getHealthCheckParams")
    void healthCheckRequest_requestIsProcessed_returnsStatusCodeAccordingToConfig(boolean givenHealthCheckConfig, HttpStatus expectedStatus) throws IOException {
        configureSource(createDefaultConfigBuilder().healthCheck(givenHealthCheckConfig).build());
        SOURCE.start(buffer);

        WebClient.of().execute(getDefaultRequestHeadersBuilder()
                        .path("/health")
                                .method(HttpMethod.GET)
                                .build(),
                        HttpData.copyOf(JsonFormat.printer().print(METRICS_REQUEST).getBytes()))
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, expectedStatus, throwable))
                .join();
    }

    private static Stream<Arguments> getHealthCheckParams() {
        return Stream.of(
                Arguments.of(true, HttpStatus.OK),
                Arguments.of(false, HttpStatus.NOT_FOUND)
        );
    }

    @Test
    void testStartWithEmptyBuffer() {
        configureSource();
        assertThrows(IllegalStateException.class, () -> SOURCE.start(null));
    }

    @ParameterizedTest
    @ArgumentsSource(BufferExceptionToStatusArgumentsProvider.class)
    void httpRequest_writingToBufferThrowsAnException_correctHttpStatusIsReturned(
            final Class<Exception> bufferExceptionClass,
            final HttpStatus expectedStatus) throws Exception {
        configureSource();
        SOURCE.start(buffer);
        doThrow(bufferExceptionClass)
                .when(buffer)
                .writeAll(anyCollection(), anyInt());

        WebClient.of()
                .execute(getDefaultRequestHeadersBuilder().build(), createJsonHttpPayload())
                .aggregate()
                .whenComplete((response, throwable) -> assertSecureResponseWithStatusCode(response, expectedStatus, throwable))
                .join();

        SOURCE.stop();
    }

    @Test
    void httpRequest_requestBodyIsTooLarge_returns413() throws InvalidProtocolBufferException {
        configureSource(createDefaultConfigBuilder().maxRequestLength(ByteCount.ofBytes(4)).build());
        SOURCE.start(buffer);

        WebClient.of()
                .execute(getDefaultRequestHeadersBuilder().build(), createJsonHttpPayload())
                .aggregate()
                .whenComplete((response, throwable) -> {
                    assertSecureResponseWithStatusCode(response, REQUEST_ENTITY_TOO_LARGE, throwable);
                })
                .join();
    }

    static class BufferExceptionToStatusArgumentsProvider implements ArgumentsProvider {
        @Override
        public Stream<? extends Arguments> provideArguments(final ExtensionContext context) {
            return Stream.of(
                    arguments(TimeoutException.class, INSUFFICIENT_STORAGE),
                    arguments(SizeOverflowException.class, INSUFFICIENT_STORAGE),
                    arguments(Exception.class, INTERNAL_SERVER_ERROR),
                    arguments(RuntimeException.class, INTERNAL_SERVER_ERROR)
            );
        }
    }

    private ExportMetricsServiceRequest createExportMetricsRequest() {
        final Resource resource = Resource.newBuilder()
                .addAttributes(KeyValue.newBuilder()
                        .setKey("service.name")
                        .setValue(AnyValue.newBuilder().setStringValue("service").build())
                ).build();

        final ResourceMetrics resourceMetrics = ResourceMetrics.newBuilder()
                .addScopeMetrics(ScopeMetrics.newBuilder()
                        .addMetrics(io.opentelemetry.proto.metrics.v1.Metric.newBuilder().build())
                        .build())
                .setResource(resource)
                .build();

        return ExportMetricsServiceRequest.newBuilder()
                .addResourceMetrics(resourceMetrics)
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

    private static Stream<Arguments> getPathParams() {
        return Stream.of(
                Arguments.of(CONFIG_HTTP_PATH, CONFIG_HTTP_PATH),
                Arguments.of("/${pipelineName}/v1/metrics", "/test_pipeline/v1/metrics")
        );
    }
}
