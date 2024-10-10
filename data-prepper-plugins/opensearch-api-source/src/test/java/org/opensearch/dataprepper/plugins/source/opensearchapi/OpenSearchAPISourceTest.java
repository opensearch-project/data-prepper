/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearchapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.AggregatedHttpResponse;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpHeaderNames;
import com.linecorp.armeria.common.HttpMethod;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.common.MediaType;
import com.linecorp.armeria.common.RequestHeaders;
import com.linecorp.armeria.common.SessionProtocol;
import com.linecorp.armeria.server.Server;
import com.linecorp.armeria.server.ServerBuilder;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Statistic;
import io.netty.util.AsciiString;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.http.BaseHttpSource;
import org.opensearch.dataprepper.http.LogThrottlingRejectHandler;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.types.ByteCount;
import org.opensearch.dataprepper.plugins.HttpBasicArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import org.opensearch.dataprepper.plugins.source.file.FileSourceConfig;
import org.opensearch.dataprepper.plugins.source.opensearchapi.model.BulkAPIEventMetadataKeyAttributes;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OpenSearchAPISourceTest {
    private static final ObjectMapper mapper = new ObjectMapper();
    private final String PLUGIN_NAME = "opensearch_api";
    private final String AUTHORITY = "127.0.0.1:9200";
    private final int DEFAULT_PORT = 9200;
    private final int DEFAULT_REQUEST_TIMEOUT_MS = 10_000;
    private final int DEFAULT_THREAD_COUNT = 200;
    private final int MAX_CONNECTIONS_COUNT = 500;
    private final int MAX_PENDING_REQUESTS_COUNT = 1024;
    private final String TEST_SSL_CERTIFICATE_FILE = getClass().getClassLoader().getResource("test_cert.crt").getFile();
    private final String TEST_SSL_KEY_FILE = getClass().getClassLoader().getResource("test_decrypted_key.key").getFile();

    private String testIndex;
    private String testPipelineName;
    private String testRoutingId;
    private String testQueryParams;

    @Mock
    private ServerBuilder serverBuilder;

    @Mock
    private Server server;

    @Mock
    private CompletableFuture<Void> completableFuture;

    private BlockingBuffer<Record<Event>> testBuffer;
    private OpenSearchAPISource openSearchAPISource;
    private List<Measurement> requestsReceivedMeasurements;
    private List<Measurement> successRequestsMeasurements;
    private List<Measurement> requestTimeoutsMeasurements;
    private List<Measurement> badRequestsMeasurements;
    private List<Measurement> requestsTooLargeMeasurements;
    private List<Measurement> rejectedRequestsMeasurements;
    private List<Measurement> requestProcessDurationMeasurements;
    private List<Measurement> payloadSizeSummaryMeasurements;
    private List<Measurement> serverConnectionsMeasurements;
    private OpenSearchAPISourceConfig sourceConfig;
    private PluginMetrics pluginMetrics;
    private PluginFactory pluginFactory;
    private PipelineDescription pipelineDescription;

    private BlockingBuffer<Record<Event>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 1);
        integerHashMap.put("batch_size", 1);
        final PluginSetting pluginSetting = new PluginSetting("blocking_buffer", integerHashMap);
        pluginSetting.setPipelineName(testPipelineName);
        return new BlockingBuffer<>(pluginSetting);
    }

    /**
     * This method should be invoked after {@link OpenSearchAPISource::start(Buffer<T> buffer)} to scrape metrics
     */
    private void refreshMeasurements() {
        final String metricNamePrefix = new StringJoiner(MetricNames.DELIMITER)
                .add(testPipelineName).add(PLUGIN_NAME).toString();
        requestsReceivedMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(OpenSearchAPIService.REQUESTS_RECEIVED).toString());
        successRequestsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(OpenSearchAPIService.SUCCESS_REQUESTS).toString());
        requestTimeoutsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(HttpRequestExceptionHandler.REQUEST_TIMEOUTS).toString());
        badRequestsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(HttpRequestExceptionHandler.BAD_REQUESTS).toString());
        requestsTooLargeMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(HttpRequestExceptionHandler.REQUESTS_TOO_LARGE).toString());
        rejectedRequestsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(LogThrottlingRejectHandler.REQUESTS_REJECTED).toString());
        requestProcessDurationMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(OpenSearchAPIService.REQUEST_PROCESS_DURATION).toString());
        payloadSizeSummaryMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(OpenSearchAPIService.PAYLOAD_SIZE).toString());
        serverConnectionsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(BaseHttpSource.SERVER_CONNECTIONS).toString());
    }

    private byte[] createGZipCompressedPayload(final String payload) throws IOException {
        // Create a GZip compressed request body
        final ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try (final GZIPOutputStream gzipStream = new GZIPOutputStream(byteStream)) {
            gzipStream.write(payload.getBytes(StandardCharsets.UTF_8));
        }
        return byteStream.toByteArray();
    }

    @BeforeEach
    public void setUp() throws Exception {
        lenient().when(serverBuilder.annotatedService(any())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.http(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.https(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);
        lenient().when(server.start()).thenReturn(completableFuture);

        sourceConfig = mock(OpenSearchAPISourceConfig.class);
        lenient().when(sourceConfig.getPort()).thenReturn(DEFAULT_PORT);
        lenient().when(sourceConfig.getPath()).thenReturn(OpenSearchAPISourceConfig.DEFAULT_ENDPOINT_URI);
        lenient().when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(DEFAULT_REQUEST_TIMEOUT_MS);
        lenient().when(sourceConfig.getThreadCount()).thenReturn(DEFAULT_THREAD_COUNT);
        lenient().when(sourceConfig.getMaxConnectionCount()).thenReturn(MAX_CONNECTIONS_COUNT);
        lenient().when(sourceConfig.getMaxPendingRequests()).thenReturn(MAX_PENDING_REQUESTS_COUNT);
        lenient().when(sourceConfig.hasHealthCheckService()).thenReturn(true);
        lenient().when(sourceConfig.getCompression()).thenReturn(CompressionOption.NONE);

        MetricsTestUtil.initMetrics();
        testPipelineName = UUID.randomUUID().toString();
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, testPipelineName);

        pluginFactory = mock(PluginFactory.class);
        final ArmeriaHttpAuthenticationProvider authenticationProvider = new HttpBasicArmeriaHttpAuthenticationProvider(new HttpBasicAuthenticationConfig("test", "test"));
        lenient().when(pluginFactory.loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(authenticationProvider);

        testBuffer = getBuffer();

        pipelineDescription = mock(PipelineDescription.class);
        lenient().when(pipelineDescription.getPipelineName()).thenReturn(testPipelineName);

        testIndex = UUID.randomUUID().toString();
        testRoutingId = UUID.randomUUID().toString();
        testQueryParams = "?pipeline=" + testPipelineName + "&routing=" + testRoutingId;
        openSearchAPISource = new OpenSearchAPISource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
    }

    @AfterEach
    public void cleanUp() {
        if (openSearchAPISource != null) {
            openSearchAPISource.stop();
        }
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

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    public void testBulkRequestAPIResponse200(boolean includeIndexInPath, boolean useQueryParams) throws IOException {
        int numberOfRecords = 1;
        testBulkRequestAPI200(includeIndexInPath, false, useQueryParams, numberOfRecords);
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    public void testBulkRequestAPICompressionResponse200(boolean includeIndexInPath, boolean useQueryParams) throws IOException {
        int numberOfRecords = 1;
        testBulkRequestAPI200(includeIndexInPath, true, useQueryParams, numberOfRecords);
    }

    @Test
    public void testHealthCheck() {
        // Prepare
        openSearchAPISource.start(testBuffer);

        // When
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority(AUTHORITY)
                        .method(HttpMethod.GET)
                        .path("/")
                        .build())
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();
    }

    @Test
    public void testHealthCheckUnauthenticatedDisabled() {
        // Prepare
        when(sourceConfig.isUnauthenticatedHealthCheck()).thenReturn(false);
        when(sourceConfig.getAuthentication()).thenReturn(new PluginModel("http_basic",
                Map.of(
                        "username", "test",
                        "password", "test"
                )));
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, testPipelineName);
        openSearchAPISource = new OpenSearchAPISource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        openSearchAPISource.start(testBuffer);

        // When
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority(AUTHORITY)
                        .method(HttpMethod.GET)
                        .path("/")
                        .build())
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.UNAUTHORIZED)).join();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestJsonResponse400WithEmptyPayload(boolean includeIndexInPath) {
        // Prepare
        final String testBadData = ""; //Empty body
        openSearchAPISource.start(testBuffer);
        refreshMeasurements();

        // When
        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority(AUTHORITY)
                                .method(HttpMethod.POST)
                                .path(includeIndexInPath ? "/" + testIndex + "/_bulk" + testQueryParams : "/_bulk" + testQueryParams)
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.ofUtf8(testBadData))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.BAD_REQUEST)).join();

        // Then
        Assertions.assertTrue(testBuffer.isEmpty());
        // Verify metrics
        final Measurement requestReceivedCount = MetricsTestUtil.getMeasurementFromList(
                requestsReceivedMeasurements, Statistic.COUNT);
        Assertions.assertEquals(1.0, requestReceivedCount.getValue());
        final Measurement badRequestsCount = MetricsTestUtil.getMeasurementFromList(
                badRequestsMeasurements, Statistic.COUNT);
        Assertions.assertEquals(1.0, badRequestsCount.getValue());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestJsonResponse400WithInvalidPayload(boolean includeIndexInPath) throws Exception {
        // Prepare
        List<String> jsonList = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Collections.singletonMap("_index", "test-index"))));
        }
        final String testBadData = String.join("\n", jsonList);
        openSearchAPISource.start(testBuffer);
        refreshMeasurements();

        // When
        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority(AUTHORITY)
                                .method(HttpMethod.POST)
                                .path(includeIndexInPath ? "/" + testIndex + "/_bulk" + testQueryParams : "/_bulk" + testQueryParams)
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.ofUtf8(testBadData))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.BAD_REQUEST)).join();

        // Then
        Assertions.assertTrue(testBuffer.isEmpty());
        // Verify metrics
        final Measurement requestReceivedCount = MetricsTestUtil.getMeasurementFromList(
                requestsReceivedMeasurements, Statistic.COUNT);
        Assertions.assertEquals(1.0, requestReceivedCount.getValue());
        final Measurement badRequestsCount = MetricsTestUtil.getMeasurementFromList(
                badRequestsMeasurements, Statistic.COUNT);
        Assertions.assertEquals(1.0, badRequestsCount.getValue());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestAPIJsonResponse413(boolean includeIndexInPath) throws JsonProcessingException {
        // Prepare
        final String testData = generateTestData(includeIndexInPath, 50);
        final int testPayloadSize = testData.getBytes().length;
        openSearchAPISource.start(testBuffer);
        refreshMeasurements();

        // When
        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority(AUTHORITY)
                                .method(HttpMethod.POST)
                                .path(includeIndexInPath ? "/" + testIndex + "/_bulk" + testQueryParams : "/_bulk" + testQueryParams)
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.ofUtf8(testData))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.REQUEST_ENTITY_TOO_LARGE)).join();

        // Then
        Assertions.assertTrue(testBuffer.isEmpty());
        // Verify metrics
        final Measurement requestReceivedCount = MetricsTestUtil.getMeasurementFromList(
                requestsReceivedMeasurements, Statistic.COUNT);
        Assertions.assertEquals(1.0, requestReceivedCount.getValue());
        final Measurement successRequestsCount = MetricsTestUtil.getMeasurementFromList(
                successRequestsMeasurements, Statistic.COUNT);
        Assertions.assertEquals(0.0, successRequestsCount.getValue());
        final Measurement requestsTooLargeCount = MetricsTestUtil.getMeasurementFromList(
                requestsTooLargeMeasurements, Statistic.COUNT);
        Assertions.assertEquals(1.0, requestsTooLargeCount.getValue());
        final Measurement requestProcessDurationCount = MetricsTestUtil.getMeasurementFromList(
                requestProcessDurationMeasurements, Statistic.COUNT);
        Assertions.assertEquals(1.0, requestProcessDurationCount.getValue());
        final Measurement requestProcessDurationMax = MetricsTestUtil.getMeasurementFromList(
                requestProcessDurationMeasurements, Statistic.MAX);
        Assertions.assertTrue(requestProcessDurationMax.getValue() > 0);
        final Measurement payloadSizeMax = MetricsTestUtil.getMeasurementFromList(
                payloadSizeSummaryMeasurements, Statistic.MAX);
        Assertions.assertEquals(testPayloadSize, payloadSizeMax.getValue());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBulkRequestAPIJsonResponse408(boolean includeIndexInPath) throws JsonProcessingException {
        // Prepare
        reset();
        final int testMaxPendingRequests = 1;
        final int testThreadCount = 1;
        final int serverTimeoutInMillis = 500;
        final int bufferTimeoutInMillis = 400;
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(serverTimeoutInMillis);
        when(sourceConfig.getBufferTimeoutInMillis()).thenReturn(bufferTimeoutInMillis);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(testMaxPendingRequests);
        when(sourceConfig.getThreadCount()).thenReturn(testThreadCount);
        openSearchAPISource = new OpenSearchAPISource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        // Start the source
        openSearchAPISource.start(testBuffer);
        refreshMeasurements();
        final RequestHeaders testRequestHeaders = RequestHeaders.builder().scheme(SessionProtocol.HTTP)
                .authority(AUTHORITY)
                .method(HttpMethod.POST)
                .path(includeIndexInPath ? "/" + testIndex + "/_bulk" + testQueryParams : "/_bulk" + testQueryParams)
                .contentType(MediaType.JSON_UTF_8)
                .build();
        final HttpData testHttpData = HttpData.ofUtf8(generateTestData(includeIndexInPath, 1));

        // Fill in the buffer
        WebClient.of().execute(testRequestHeaders, testHttpData).aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();

        // Disable client timeout
        WebClient testWebClient = WebClient.builder().responseTimeoutMillis(0).build();

        // When/Then
        testWebClient.execute(testRequestHeaders, testHttpData)
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.REQUEST_TIMEOUT)).join();
        // verify metrics
        final Measurement requestReceivedCount = MetricsTestUtil.getMeasurementFromList(
                requestsReceivedMeasurements, Statistic.COUNT);
        Assertions.assertEquals(2.0, requestReceivedCount.getValue());
        final Measurement successRequestsCount = MetricsTestUtil.getMeasurementFromList(
                successRequestsMeasurements, Statistic.COUNT);
        Assertions.assertEquals(1.0, successRequestsCount.getValue());
        final Measurement requestTimeoutsCount = MetricsTestUtil.getMeasurementFromList(
                requestTimeoutsMeasurements, Statistic.COUNT);
        Assertions.assertEquals(1.0, requestTimeoutsCount.getValue());
        final Measurement requestProcessDurationMax = MetricsTestUtil.getMeasurementFromList(
                requestProcessDurationMeasurements, Statistic.MAX);
        final double maxDurationInMillis = 1000 * requestProcessDurationMax.getValue();
        Assertions.assertTrue(maxDurationInMillis > bufferTimeoutInMillis);
    }

    private void testBulkRequestAPI200(boolean includeIndexInPath, boolean useCompression, boolean useQueryParams, int numberOfRecords) throws IOException {
        final String testData = generateTestData(includeIndexInPath, numberOfRecords);
        final int testPayloadSize = testData.getBytes().length;
        if (useCompression) {
            when(sourceConfig.getCompression()).thenReturn(CompressionOption.GZIP);
        }

        openSearchAPISource = new OpenSearchAPISource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        openSearchAPISource.start(testBuffer);
        refreshMeasurements();

        final String queryParams = useQueryParams ? testQueryParams : "";
        // When
        if (useCompression) {
            WebClient.of().execute(RequestHeaders.builder()
                                    .scheme(SessionProtocol.HTTP)
                                    .authority(AUTHORITY)
                                    .method(HttpMethod.POST)
                                    .path(includeIndexInPath ? "/" + testIndex + "/_bulk" + queryParams : "/_bulk" + queryParams)
                                    .add(HttpHeaderNames.CONTENT_ENCODING, "gzip")
                                    .build(),
                            createGZipCompressedPayload(testData))
                    .aggregate()
                    .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();
        } else {
            WebClient.of().execute(RequestHeaders.builder()
                                    .scheme(SessionProtocol.HTTP)
                                    .authority(AUTHORITY)
                                    .method(HttpMethod.POST)
                                    .path(includeIndexInPath ? "/" + testIndex + "/_bulk" + testQueryParams : "/_bulk" + testQueryParams)
                                    .contentType(MediaType.JSON_UTF_8)
                                    .build(),
                            HttpData.ofUtf8(testData))
                    .aggregate()
                    .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();
        }
        // Then
        Assertions.assertFalse(testBuffer.isEmpty());

        final Map.Entry<Collection<Record<Event>>, CheckpointState> result = testBuffer.read(100);
        List<Record<Event>> records = new ArrayList<>(result.getKey());
        Assertions.assertEquals(numberOfRecords, records.size());
        final Record<Event> record = records.get(0);
        Assertions.assertEquals("text-data", record.getData().get("text", String.class));
        Assertions.assertEquals("index", record.getData().getMetadata().getAttribute(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ACTION));
        Assertions.assertEquals(testIndex, record.getData().getMetadata().getAttribute(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_INDEX));
        Assertions.assertEquals("123", record.getData().getMetadata().getAttribute(BulkAPIEventMetadataKeyAttributes.BULK_API_EVENT_METADATA_ATTRIBUTE_ID));
        // Verify metrics
        final Measurement requestReceivedCount = MetricsTestUtil.getMeasurementFromList(
                requestsReceivedMeasurements, Statistic.COUNT);
        Assertions.assertEquals(1.0, requestReceivedCount.getValue());
        final Measurement successRequestsCount = MetricsTestUtil.getMeasurementFromList(
                successRequestsMeasurements, Statistic.COUNT);
        Assertions.assertEquals(1.0, successRequestsCount.getValue());
        final Measurement requestProcessDurationCount = MetricsTestUtil.getMeasurementFromList(
                requestProcessDurationMeasurements, Statistic.COUNT);
        Assertions.assertEquals(1.0, requestProcessDurationCount.getValue());
        final Measurement requestProcessDurationMax = MetricsTestUtil.getMeasurementFromList(
                requestProcessDurationMeasurements, Statistic.MAX);
        Assertions.assertTrue(requestProcessDurationMax.getValue() > 0);
        final Measurement payloadSizeMax = MetricsTestUtil.getMeasurementFromList(
                payloadSizeSummaryMeasurements, Statistic.MAX);
        Assertions.assertEquals(testPayloadSize, payloadSizeMax.getValue());
        Assertions.assertTrue(requestProcessDurationMax.getValue() > 0);
    }

    private String generateTestData(boolean includeIndexInPath, int numberOfRecords) throws JsonProcessingException {
        List<String> jsonList = new ArrayList<>();
        for (int i = 0; i < numberOfRecords; i++) {
            if (includeIndexInPath) {
                jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Map.of("_id", "123"))));
            } else {
                jsonList.add(mapper.writeValueAsString(Collections.singletonMap("index", Map.of("_index", testIndex, "_id", "123"))));
            }
            jsonList.add(mapper.writeValueAsString(Collections.singletonMap("text", "text-data")));
        }
        return String.join("\n", jsonList);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testOpenSearchAPISourceServerConnectionsMetric(boolean includeIndexInPath) throws JsonProcessingException {
        // Prepare
        openSearchAPISource = new OpenSearchAPISource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        openSearchAPISource.start(testBuffer);
        refreshMeasurements();

        // Verify connections metric value is 0
        Measurement serverConnectionsMeasurement = MetricsTestUtil.getMeasurementFromList(serverConnectionsMeasurements, Statistic.VALUE);
        Assertions.assertEquals(0, serverConnectionsMeasurement.getValue());

        final RequestHeaders testRequestHeaders = RequestHeaders.builder().scheme(SessionProtocol.HTTP)
                .authority(AUTHORITY)
                .method(HttpMethod.POST)
                .path(includeIndexInPath ? "/" + testIndex + "/_bulk" + testQueryParams : "/_bulk" + testQueryParams)
                .contentType(MediaType.JSON_UTF_8)
                .build();
        final HttpData testHttpData = HttpData.ofUtf8(generateTestData(includeIndexInPath, 1));

        // Send request
        WebClient.of().execute(testRequestHeaders, testHttpData).aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();

        // Verify connections metric value is 1
        serverConnectionsMeasurement = MetricsTestUtil.getMeasurementFromList(serverConnectionsMeasurements, Statistic.VALUE);
        Assertions.assertEquals(1.0, serverConnectionsMeasurement.getValue());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testBulkRequestAPIJsonResponse(boolean includeIndexInPath) throws JsonProcessingException {
        reset(sourceConfig);
        when(sourceConfig.getPort()).thenReturn(DEFAULT_PORT);
        when(sourceConfig.getPath()).thenReturn(OpenSearchAPISourceConfig.DEFAULT_ENDPOINT_URI);
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(DEFAULT_REQUEST_TIMEOUT_MS);
        when(sourceConfig.getThreadCount()).thenReturn(DEFAULT_THREAD_COUNT);
        when(sourceConfig.getMaxConnectionCount()).thenReturn(MAX_CONNECTIONS_COUNT);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(MAX_PENDING_REQUESTS_COUNT);
        when(sourceConfig.isSsl()).thenReturn(true);
        when(sourceConfig.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
        when(sourceConfig.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);
        openSearchAPISource = new OpenSearchAPISource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        testBuffer = getBuffer();
        openSearchAPISource.start(testBuffer);

        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTPS)
                                .authority(AUTHORITY)
                                .method(HttpMethod.POST)
                                .path(includeIndexInPath ? "/" + testIndex + "/_bulk" + testQueryParams : "/_bulk" + testQueryParams)
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.ofUtf8(generateTestData(includeIndexInPath, 1)))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();
    }

    @Test
    public void request_that_exceeds_maxRequestLength_returns_413() throws JsonProcessingException {
        reset(sourceConfig);
        when(sourceConfig.getPort()).thenReturn(DEFAULT_PORT);
        when(sourceConfig.getPath()).thenReturn(OpenSearchAPISourceConfig.DEFAULT_ENDPOINT_URI);
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(10_000);
        when(sourceConfig.getThreadCount()).thenReturn(200);
        when(sourceConfig.getMaxConnectionCount()).thenReturn(500);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(1024);
        when(sourceConfig.hasHealthCheckService()).thenReturn(false);
        when(sourceConfig.getCompression()).thenReturn(CompressionOption.NONE);
        when(sourceConfig.getMaxRequestLength()).thenReturn(ByteCount.ofBytes(4));
        openSearchAPISource = new OpenSearchAPISource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        // Prepare
        final String testData = "" +
                "{ \"index\": { \"_index\": \"test-index\", \"_id\": \"id1\" } }\n" +
                "{ \"text\": \"text1\", \"year\": \"2013\" }";

        assertThat((long) testData.getBytes().length, greaterThan(sourceConfig.getMaxRequestLength().getBytes()));
        openSearchAPISource.start(testBuffer);
        refreshMeasurements();

        // When
        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority(AUTHORITY)
                                .method(HttpMethod.POST)
                                .path("/")
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.ofUtf8(testData))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.REQUEST_ENTITY_TOO_LARGE)).join();

        // Then
        Assertions.assertTrue(testBuffer.isEmpty());
    }
}
