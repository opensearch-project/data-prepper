/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.source.prometheus;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.ClientFactory;
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
import com.arpnetworking.metrics.prometheus.Remote;
import com.arpnetworking.metrics.prometheus.Types;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.netty.util.AsciiString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.HttpBasicArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBufferConfig;
import org.xerial.snappy.Snappy;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PrometheusRemoteWriteSourceTest {

    private static final String PLUGIN_NAME = "prometheus";
    private static final String TEST_PIPELINE_NAME = "test_pipeline";
    private static final int TEST_PORT = 9201;
    private static final String TEST_USERNAME = "test_user";
    private static final String TEST_PASSWORD = "test_password";
    private final String TEST_SSL_CERTIFICATE_FILE = Objects.requireNonNull(
            getClass().getClassLoader().getResource("test_cert.crt"),
            "test_cert.crt not found in test resources").getFile();
    private final String TEST_SSL_KEY_FILE = Objects.requireNonNull(
            getClass().getClassLoader().getResource("test_decrypted_key.key"),
            "test_decrypted_key.key not found in test resources").getFile();

    @Mock
    private PipelineDescription pipelineDescription;

    private BlockingBuffer<Record<Event>> testBuffer;
    private PrometheusRemoteWriteSource sourceUnderTest;
    private PrometheusRemoteWriteSourceConfig sourceConfig;
    private PluginMetrics pluginMetrics;
    private PluginFactory pluginFactory;

    private List<Measurement> requestsReceivedMeasurements;
    private List<Measurement> successRequestsMeasurements;
    private List<Measurement> requestProcessDurationMeasurements;
    private List<Measurement> payloadSizeSummaryMeasurements;

    private static void initMetrics() {
        final Set<MeterRegistry> registries = new HashSet<>(Metrics.globalRegistry.getRegistries());
        registries.forEach(Metrics.globalRegistry::remove);

        final List<Meter> meters = new ArrayList<>(Metrics.globalRegistry.getMeters());
        meters.forEach(Metrics.globalRegistry::remove);

        Metrics.addRegistry(new SimpleMeterRegistry());
    }

    private static List<Measurement> getMeasurementList(final String meterName) {
        final MeterRegistry registry = Metrics.globalRegistry.getRegistries().iterator().next();
        final Meter meter = registry.find(meterName).meter();
        if (meter == null) {
            throw new RuntimeException("No metrics meter is available for " + meterName);
        }
        return StreamSupport.stream(meter.measure().spliterator(), false)
                .collect(Collectors.toList());
    }

    private static Measurement getMeasurementFromList(final List<Measurement> measurements, final Statistic statistic) {
        return measurements.stream().filter(m -> m.getStatistic() == statistic).findAny().get();
    }

    private BlockingBuffer<Record<Event>> getBuffer(final int bufferSize, final int batchSize) throws JsonProcessingException {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", bufferSize);
        integerHashMap.put("batch_size", batchSize);
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(integerHashMap);
        BlockingBufferConfig blockingBufferConfig = objectMapper.readValue(json, BlockingBufferConfig.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        return new BlockingBuffer<>(blockingBufferConfig, pipelineDescription);
    }

    private void refreshMeasurements() {
        final String metricNamePrefix = new StringJoiner(MetricNames.DELIMITER)
                .add(TEST_PIPELINE_NAME).add(PLUGIN_NAME).toString();
        requestsReceivedMeasurements = getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(PrometheusRemoteWriteService.REQUESTS_RECEIVED).toString());
        successRequestsMeasurements = getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(PrometheusRemoteWriteService.SUCCESS_REQUESTS).toString());
        requestProcessDurationMeasurements = getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(PrometheusRemoteWriteService.REQUEST_PROCESS_DURATION).toString());
        payloadSizeSummaryMeasurements = getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(PrometheusRemoteWriteService.PAYLOAD_SIZE).toString());
    }

    private byte[] createValidSnappyProtobuf() throws Exception {
        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("test_metric").build())
                .addLabels(Types.Label.newBuilder().setName("host").setValue("localhost").build())
                .addSamples(Types.Sample.newBuilder().setValue(42.0).setTimestamp(System.currentTimeMillis()).build())
                .build();

        final Remote.WriteRequest writeRequest = Remote.WriteRequest.newBuilder()
                .addTimeseries(timeSeries)
                .build();

        return Snappy.compress(writeRequest.toByteArray());
    }

    @BeforeEach
    public void setUp() throws JsonProcessingException {
        sourceConfig = mock(PrometheusRemoteWriteSourceConfig.class);
        lenient().when(sourceConfig.getPort()).thenReturn(TEST_PORT);
        lenient().when(sourceConfig.getPath()).thenReturn("/api/v1/write");
        lenient().when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(10_000);
        lenient().when(sourceConfig.getBufferTimeoutInMillis()).thenReturn(8000);
        lenient().when(sourceConfig.getThreadCount()).thenReturn(200);
        lenient().when(sourceConfig.getMaxConnectionCount()).thenReturn(500);
        lenient().when(sourceConfig.getMaxPendingRequests()).thenReturn(1024);
        lenient().when(sourceConfig.hasHealthCheckService()).thenReturn(true);
        lenient().when(sourceConfig.isUnauthenticatedHealthCheck()).thenReturn(true);
        lenient().when(sourceConfig.isSsl()).thenReturn(false);
        lenient().when(sourceConfig.getCompression()).thenReturn(null);
        lenient().when(sourceConfig.getMaxRequestLength()).thenReturn(null);
        lenient().when(sourceConfig.getAuthentication()).thenReturn(null);
        lenient().when(sourceConfig.isFlattenLabels()).thenReturn(false);

        initMetrics();
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        pluginFactory = mock(PluginFactory.class);
        final ArmeriaHttpAuthenticationProvider authenticationProvider = mock(ArmeriaHttpAuthenticationProvider.class);
        when(pluginFactory.loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(authenticationProvider);

        testBuffer = getBuffer(10, 10);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        sourceUnderTest = new PrometheusRemoteWriteSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
    }

    @AfterEach
    public void cleanUp() {
        if (sourceUnderTest != null) {
            sourceUnderTest.stop();
        }
    }

    @Test
    public void testProtobufResponse200() throws Exception {
        final byte[] testPayload = createValidSnappyProtobuf();
        sourceUnderTest.start(testBuffer);
        refreshMeasurements();

        final AggregatedHttpResponse response = WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:" + TEST_PORT)
                        .method(HttpMethod.POST)
                        .path("/api/v1/write")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.wrap(testPayload))
                .aggregate().join();

        assertEquals(HttpStatus.OK, response.status());

        final Measurement requestReceivedCount = getMeasurementFromList(
                requestsReceivedMeasurements, Statistic.COUNT);
        assertEquals(1.0, requestReceivedCount.getValue());
        final Measurement successRequestsCount = getMeasurementFromList(
                successRequestsMeasurements, Statistic.COUNT);
        assertEquals(1.0, successRequestsCount.getValue());
        final Measurement requestProcessDurationCount = getMeasurementFromList(
                requestProcessDurationMeasurements, Statistic.COUNT);
        assertEquals(1.0, requestProcessDurationCount.getValue());
        final Measurement payloadSizeMax = getMeasurementFromList(
                payloadSizeSummaryMeasurements, Statistic.MAX);
        assertEquals(testPayload.length, payloadSizeMax.getValue());
    }

    @Test
    public void testHealthCheck() {
        sourceUnderTest.start(testBuffer);

        final AggregatedHttpResponse response = WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:" + TEST_PORT)
                        .method(HttpMethod.GET)
                        .path("/health")
                        .build())
                .aggregate().join();

        assertEquals(HttpStatus.OK, response.status());
    }

    @Test
    public void testInvalidProtobufResponse400() throws Exception {
        final byte[] invalidPayload = "not valid protobuf".getBytes();
        sourceUnderTest.start(testBuffer);
        refreshMeasurements();

        final AggregatedHttpResponse response = WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:" + TEST_PORT)
                        .method(HttpMethod.POST)
                        .path("/api/v1/write")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.wrap(invalidPayload))
                .aggregate().join();

        assertEquals(HttpStatus.BAD_REQUEST, response.status());
    }

    @Test
    public void testServerConnectionsMetric() {
        sourceUnderTest.start(testBuffer);
        refreshMeasurements();

        final List<Measurement> serverConnectionsMeasurements = getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER)
                        .add(TEST_PIPELINE_NAME).add(PLUGIN_NAME)
                        .add("serverConnections").toString());
        final Measurement serverConnectionsMeasurement = getMeasurementFromList(
                serverConnectionsMeasurements, Statistic.VALUE);
        assertEquals(0, serverConnectionsMeasurement.getValue());
    }

    @Test
    public void testStartWithEmptyBuffer() {
        final PrometheusRemoteWriteSource source = new PrometheusRemoteWriteSource(
                sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        assertThrows(IllegalStateException.class, () -> source.start(null));
    }

    @Test
    public void testStopWithoutStart() {
        final PrometheusRemoteWriteSource source = new PrometheusRemoteWriteSource(
                sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        source.stop();
    }

    @Test
    public void testRunAnotherSourceWithSamePort() {
        sourceUnderTest.start(testBuffer);
        final PrometheusRemoteWriteSource secondSource = new PrometheusRemoteWriteSource(
                sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        assertThrows(RuntimeException.class, () -> secondSource.start(testBuffer));
    }

    @Test
    public void testConstructorWithNullAuthentication() {
        when(sourceConfig.getAuthentication()).thenReturn(null);

        final PrometheusRemoteWriteSource source = new PrometheusRemoteWriteSource(
                sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        assertNotNull(source);
    }

    @Test
    public void testConstructorWithUnauthenticatedPluginName() {
        final PluginModel authConfig = mock(PluginModel.class);
        when(authConfig.getPluginName()).thenReturn(ArmeriaHttpAuthenticationProvider.UNAUTHENTICATED_PLUGIN_NAME);
        when(authConfig.getPluginSettings()).thenReturn(Collections.emptyMap());
        when(sourceConfig.getAuthentication()).thenReturn(authConfig);

        final PrometheusRemoteWriteSource source = new PrometheusRemoteWriteSource(
                sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        assertNotNull(source);
    }

    @Test
    public void testConstructorWithNamedAuthenticationPlugin() {
        final PluginModel authConfig = mock(PluginModel.class);
        when(authConfig.getPluginName()).thenReturn("http_basic");
        when(authConfig.getPluginSettings()).thenReturn(Map.of("username", "test"));
        when(sourceConfig.getAuthentication()).thenReturn(authConfig);

        final PrometheusRemoteWriteSource source = new PrometheusRemoteWriteSource(
                sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        assertNotNull(source);
    }

    private PrometheusRemoteWriteSource createSourceWithBasicAuth() throws JsonProcessingException {
        when(sourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of("username", TEST_USERNAME, "password", TEST_PASSWORD)));

        final ArmeriaHttpAuthenticationProvider authProvider =
                new HttpBasicArmeriaHttpAuthenticationProvider(
                        new HttpBasicAuthenticationConfig(TEST_USERNAME, TEST_PASSWORD));
        when(pluginFactory.loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(authProvider);

        initMetrics();
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);
        return new PrometheusRemoteWriteSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
    }

    private void assertSecureResponseWithStatusCode(final AggregatedHttpResponse response, final HttpStatus expectedStatus) {
        assertThat("Http Status", response.status(), equalTo(expectedStatus));

        final List<String> headerKeys = response.headers()
                .stream()
                .map(Map.Entry::getKey)
                .map(AsciiString::toString)
                .collect(Collectors.toList());
        assertThat("Response Header Keys", headerKeys, not(contains("server")));
    }

    @Test
    public void testUnauthenticatedRequestReturns401() throws Exception {
        sourceUnderTest.stop();
        sourceUnderTest = createSourceWithBasicAuth();
        sourceUnderTest.start(testBuffer);

        final byte[] payload = createValidSnappyProtobuf();
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:" + TEST_PORT)
                        .method(HttpMethod.POST)
                        .path("/api/v1/write")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.wrap(payload))
                .aggregate()
                .whenComplete((response, ex) -> {
                    if (ex != null) {
                        throw new RuntimeException("HTTP request failed", ex);
                    }
                    assertSecureResponseWithStatusCode(response, HttpStatus.UNAUTHORIZED);
                })
                .join();
    }

    @Test
    public void testInvalidCredentialsReturns401() throws Exception {
        sourceUnderTest.stop();
        sourceUnderTest = createSourceWithBasicAuth();
        sourceUnderTest.start(testBuffer);

        final String invalidCredentials = Base64.getEncoder()
                .encodeToString("wrong_user:wrong_password".getBytes(StandardCharsets.UTF_8));

        final byte[] payload = createValidSnappyProtobuf();
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:" + TEST_PORT)
                        .method(HttpMethod.POST)
                        .path("/api/v1/write")
                        .contentType(MediaType.PROTOBUF)
                        .add(HttpHeaderNames.AUTHORIZATION, "Basic " + invalidCredentials)
                        .build(),
                HttpData.wrap(payload))
                .aggregate()
                .whenComplete((response, ex) -> {
                    if (ex != null) {
                        throw new RuntimeException("HTTP request failed", ex);
                    }
                    assertSecureResponseWithStatusCode(response, HttpStatus.UNAUTHORIZED);
                })
                .join();
    }

    @Test
    public void testValidCredentialsReturns200() throws Exception {
        sourceUnderTest.stop();
        sourceUnderTest = createSourceWithBasicAuth();
        sourceUnderTest.start(testBuffer);

        final String validCredentials = Base64.getEncoder()
                .encodeToString(String.format("%s:%s", TEST_USERNAME, TEST_PASSWORD)
                        .getBytes(StandardCharsets.UTF_8));

        final byte[] payload = createValidSnappyProtobuf();
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:" + TEST_PORT)
                        .method(HttpMethod.POST)
                        .path("/api/v1/write")
                        .contentType(MediaType.PROTOBUF)
                        .add(HttpHeaderNames.AUTHORIZATION, "Basic " + validCredentials)
                        .build(),
                HttpData.wrap(payload))
                .aggregate()
                .whenComplete((response, ex) -> {
                    if (ex != null) {
                        throw new RuntimeException("HTTP request failed", ex);
                    }
                    assertSecureResponseWithStatusCode(response, HttpStatus.OK);
                })
                .join();
    }

    @Test
    public void testHealthCheckUnauthenticatedAllowedWithAuthEnabled() throws Exception {
        when(sourceConfig.isUnauthenticatedHealthCheck()).thenReturn(true);
        sourceUnderTest.stop();
        sourceUnderTest = createSourceWithBasicAuth();
        sourceUnderTest.start(testBuffer);

        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:" + TEST_PORT)
                        .method(HttpMethod.GET)
                        .path("/health")
                        .build())
                .aggregate()
                .whenComplete((response, ex) -> {
                    if (ex != null) {
                        throw new RuntimeException("HTTP request failed", ex);
                    }
                    assertSecureResponseWithStatusCode(response, HttpStatus.OK);
                })
                .join();
    }

    @Test
    public void testHealthCheckUnauthenticatedDisabledWithAuthEnabled() throws Exception {
        when(sourceConfig.isUnauthenticatedHealthCheck()).thenReturn(false);
        sourceUnderTest.stop();
        sourceUnderTest = createSourceWithBasicAuth();
        sourceUnderTest.start(testBuffer);

        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:" + TEST_PORT)
                        .method(HttpMethod.GET)
                        .path("/health")
                        .build())
                .aggregate()
                .whenComplete((response, ex) -> {
                    if (ex != null) {
                        throw new RuntimeException("HTTP request failed", ex);
                    }
                    assertSecureResponseWithStatusCode(response, HttpStatus.UNAUTHORIZED);
                })
                .join();
    }

    @Test
    public void testHttpsWithValidCertificateReturns200() throws Exception {
        sourceUnderTest.stop();
        when(sourceConfig.isSsl()).thenReturn(true);
        when(sourceConfig.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
        when(sourceConfig.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);
        sourceUnderTest = new PrometheusRemoteWriteSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        sourceUnderTest.start(testBuffer);

        final byte[] payload = createValidSnappyProtobuf();
        WebClient.builder().factory(ClientFactory.insecure()).build()
                .execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTPS)
                        .authority("127.0.0.1:" + TEST_PORT)
                        .method(HttpMethod.POST)
                        .path("/api/v1/write")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.wrap(payload))
                .aggregate()
                .whenComplete((response, ex) -> {
                    if (ex != null) {
                        throw new RuntimeException("HTTP request failed", ex);
                    }
                    assertSecureResponseWithStatusCode(response, HttpStatus.OK);
                })
                .join();
    }

    @Test
    public void testHttpRequestFailsWhenSslIsEnabled() throws Exception {
        sourceUnderTest.stop();
        when(sourceConfig.isSsl()).thenReturn(true);
        when(sourceConfig.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
        when(sourceConfig.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);
        sourceUnderTest = new PrometheusRemoteWriteSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        sourceUnderTest.start(testBuffer);

        final byte[] payload = createValidSnappyProtobuf();
        final CompletableFuture<AggregatedHttpResponse> future = WebClient.builder()
                .factory(ClientFactory.insecure())
                .build()
                .execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:" + TEST_PORT)
                        .method(HttpMethod.POST)
                        .path("/api/v1/write")
                        .contentType(MediaType.PROTOBUF)
                        .build(),
                HttpData.wrap(payload))
                .aggregate();

        final ExecutionException exception = assertThrows(ExecutionException.class,
                () -> future.get(2, TimeUnit.SECONDS));
        assertInstanceOf(ClosedSessionException.class, exception.getCause());
    }

    @Test
    public void testStartWithScrapeConfigCreatesScrapeService() throws Exception {
        sourceUnderTest.stop();

        final PrometheusScrapeConfig scrapeConfig = mock(PrometheusScrapeConfig.class);
        lenient().when(scrapeConfig.getScrapeInterval()).thenReturn(Duration.ofDays(1));
        lenient().when(scrapeConfig.getScrapeTimeout()).thenReturn(Duration.ofSeconds(10));
        lenient().when(scrapeConfig.isFlattenLabels()).thenReturn(false);
        lenient().when(scrapeConfig.isInsecure()).thenReturn(false);
        lenient().when(scrapeConfig.getAuthentication()).thenReturn(null);
        lenient().when(scrapeConfig.getTargets()).thenReturn(Collections.emptyList());
        when(sourceConfig.getScrapeConfig()).thenReturn(scrapeConfig);

        sourceUnderTest = new PrometheusRemoteWriteSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        sourceUnderTest.start(testBuffer);

        final Field scrapeServiceField =
                PrometheusRemoteWriteSource.class.getDeclaredField("scrapeService");
        scrapeServiceField.setAccessible(true);
        assertThat(scrapeServiceField.get(sourceUnderTest), notNullValue());
    }

    @Test
    public void testStopWithScrapeServiceStopsScrapeService() throws Exception {
        sourceUnderTest.stop();

        final PrometheusScrapeConfig scrapeConfig = mock(PrometheusScrapeConfig.class);
        lenient().when(scrapeConfig.getScrapeInterval()).thenReturn(Duration.ofDays(1));
        lenient().when(scrapeConfig.getScrapeTimeout()).thenReturn(Duration.ofSeconds(10));
        lenient().when(scrapeConfig.isFlattenLabels()).thenReturn(false);
        lenient().when(scrapeConfig.isInsecure()).thenReturn(false);
        lenient().when(scrapeConfig.getAuthentication()).thenReturn(null);
        lenient().when(scrapeConfig.getTargets()).thenReturn(Collections.emptyList());
        when(sourceConfig.getScrapeConfig()).thenReturn(scrapeConfig);

        sourceUnderTest = new PrometheusRemoteWriteSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        sourceUnderTest.start(testBuffer);
        sourceUnderTest.stop();

        final Field scrapeServiceField =
                PrometheusRemoteWriteSource.class.getDeclaredField("scrapeService");
        scrapeServiceField.setAccessible(true);
        assertThat(scrapeServiceField.get(sourceUnderTest), notNullValue());
        sourceUnderTest = null;
    }

    @Test
    public void testStartWithoutScrapeConfigDoesNotCreateScrapeService() throws Exception {
        sourceUnderTest.stop();

        when(sourceConfig.getScrapeConfig()).thenReturn(null);

        sourceUnderTest = new PrometheusRemoteWriteSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        sourceUnderTest.start(testBuffer);

        final Field scrapeServiceField =
                PrometheusRemoteWriteSource.class.getDeclaredField("scrapeService");
        scrapeServiceField.setAccessible(true);
        assertThat(scrapeServiceField.get(sourceUnderTest), nullValue());
    }

    @Test
    public void testStopWithoutScrapeServiceDoesNotThrow() {
        sourceUnderTest.stop();

        when(sourceConfig.getScrapeConfig()).thenReturn(null);
        sourceUnderTest = new PrometheusRemoteWriteSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        sourceUnderTest.start(testBuffer);
        sourceUnderTest.stop();
        sourceUnderTest = null;
    }
}