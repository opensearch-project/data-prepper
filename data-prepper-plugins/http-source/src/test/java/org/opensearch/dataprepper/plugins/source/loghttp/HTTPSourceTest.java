/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.loghttp;

import com.linecorp.armeria.common.HttpHeaderNames;
import org.opensearch.dataprepper.HttpRequestExceptionHandler;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.MetricsTestUtil;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.codec.CompressionOption;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.ResponseTimeoutException;
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
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Statistic;
import io.netty.util.AsciiString;
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
import org.opensearch.dataprepper.armeria.authentication.ArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.armeria.authentication.HttpBasicAuthenticationConfig;
import org.opensearch.dataprepper.plugins.HttpBasicArmeriaHttpAuthenticationProvider;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.zip.GZIPOutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HTTPSourceTest {
    /**
     * TODO: according to the new coding guideline, consider refactoring the following test cases into HTTPSourceIT.
     * - testHTTPJsonResponse200()
     * - testHTTPJsonResponse400()
     * - testHTTPJsonResponse413()
     * - testHTTPJsonResponse415()
     * - testHTTPJsonResponse429()
     * - testHTTPSJsonResponse()
     */
    private final String PLUGIN_NAME = "http";
    private final String TEST_PIPELINE_NAME = "test_pipeline";
    private final String TEST_SSL_CERTIFICATE_FILE = getClass().getClassLoader().getResource("test_cert.crt").getFile();
    private final String TEST_SSL_KEY_FILE = getClass().getClassLoader().getResource("test_decrypted_key.key").getFile();

    @Mock
    private ServerBuilder serverBuilder;

    @Mock
    private Server server;

    @Mock
    private CompletableFuture<Void> completableFuture;

    private BlockingBuffer<Record<Log>> testBuffer;
    private HTTPSource HTTPSourceUnderTest;
    private List<Measurement> requestsReceivedMeasurements;
    private List<Measurement> successRequestsMeasurements;
    private List<Measurement> requestTimeoutsMeasurements;
    private List<Measurement> badRequestsMeasurements;
    private List<Measurement> requestsTooLargeMeasurements;
    private List<Measurement> rejectedRequestsMeasurements;
    private List<Measurement> requestProcessDurationMeasurements;
    private List<Measurement> payloadSizeSummaryMeasurements;
    private HTTPSourceConfig sourceConfig;
    private PluginMetrics pluginMetrics;
    private PluginFactory pluginFactory;
    private PipelineDescription pipelineDescription;

    private BlockingBuffer<Record<Log>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 1);
        integerHashMap.put("batch_size", 1);
        final PluginSetting pluginSetting = new PluginSetting("blocking_buffer", integerHashMap);
        pluginSetting.setPipelineName(TEST_PIPELINE_NAME);
        return new BlockingBuffer<>(pluginSetting);
    }

    /**
     * This method should be invoked after {@link HTTPSource::start(Buffer<T> buffer)} to scrape metrics
     */
    private void refreshMeasurements() {
        final String metricNamePrefix = new StringJoiner(MetricNames.DELIMITER)
                .add(TEST_PIPELINE_NAME).add(PLUGIN_NAME).toString();
        requestsReceivedMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(LogHTTPService.REQUESTS_RECEIVED).toString());
        successRequestsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(LogHTTPService.SUCCESS_REQUESTS).toString());
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
                        .add(LogHTTPService.REQUEST_PROCESS_DURATION).toString());
        payloadSizeSummaryMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(LogHTTPService.PAYLOAD_SIZE).toString());
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
    public void setUp() {
        lenient().when(serverBuilder.annotatedService(any())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.http(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.https(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);
        lenient().when(server.start()).thenReturn(completableFuture);

        sourceConfig = mock(HTTPSourceConfig.class);
        lenient().when(sourceConfig.getPort()).thenReturn(2021);
        lenient().when(sourceConfig.getPath()).thenReturn(HTTPSourceConfig.DEFAULT_LOG_INGEST_URI);
        lenient().when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(10_000);
        lenient().when(sourceConfig.getThreadCount()).thenReturn(200);
        lenient().when(sourceConfig.getMaxConnectionCount()).thenReturn(500);
        lenient().when(sourceConfig.getMaxPendingRequests()).thenReturn(1024);
        lenient().when(sourceConfig.hasHealthCheckService()).thenReturn(true);
        lenient().when(sourceConfig.getCompression()).thenReturn(CompressionOption.NONE);

        MetricsTestUtil.initMetrics();
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        pluginFactory = mock(PluginFactory.class);
        final ArmeriaHttpAuthenticationProvider authenticationProvider = new HttpBasicArmeriaHttpAuthenticationProvider(new HttpBasicAuthenticationConfig("test", "test"));
        when(pluginFactory.loadPlugin(eq(ArmeriaHttpAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(authenticationProvider);

        testBuffer = getBuffer();
        pipelineDescription = mock(PipelineDescription.class);
        when(pipelineDescription.getPipelineName()).thenReturn(TEST_PIPELINE_NAME);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
    }

    @AfterEach
    public void cleanUp() {
        if (HTTPSourceUnderTest != null) {
            HTTPSourceUnderTest.stop();
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

    @Test
    public void testHTTPJsonResponse200() {
        // Prepare
        final String testData = "[{\"log\": \"somelog\"}]";
        final int testPayloadSize = testData.getBytes().length;
        HTTPSourceUnderTest.start(testBuffer);
        refreshMeasurements();

        // When
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:2021")
                        .method(HttpMethod.POST)
                        .path("/log/ingest")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.ofUtf8(testData))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();

        // Then
        Assertions.assertFalse(testBuffer.isEmpty());

        final Map.Entry<Collection<Record<Log>>, CheckpointState> result = testBuffer.read(100);
        List<Record<Log>> records = new ArrayList<>(result.getKey());
        Assertions.assertEquals(1, records.size());
        final Record<Log> record = records.get(0);
        Assertions.assertEquals("somelog", record.getData().get("log", String.class));
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
    }

    @Test
    public void testHttpCompressionResponse200() throws IOException {
        // Prepare
        final String testData = "[{\"log\": \"somelog\"}]";
        when(sourceConfig.getCompression()).thenReturn(CompressionOption.GZIP);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        HTTPSourceUnderTest.start(testBuffer);
        refreshMeasurements();

        // When
        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority("127.0.0.1:2021")
                                .method(HttpMethod.POST)
                                .path("/log/ingest")
                                .add(HttpHeaderNames.CONTENT_ENCODING, "gzip")
                                .build(),
                        createGZipCompressedPayload(testData))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();

        // Then
        Assertions.assertFalse(testBuffer.isEmpty());

        final Map.Entry<Collection<Record<Log>>, CheckpointState> result = testBuffer.read(100);
        List<Record<Log>> records = new ArrayList<>(result.getKey());
        Assertions.assertEquals(1, records.size());
        final Record<Log> record = records.get(0);
        Assertions.assertEquals("somelog", record.getData().get("log", String.class));
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
    }

    @Test
    public void testHealthCheck() {
        // Prepare
        HTTPSourceUnderTest.start(testBuffer);

        // When
        WebClient.of().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTP)
                                .authority("127.0.0.1:2021")
                                .method(HttpMethod.GET)
                                .path("/health")
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
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        HTTPSourceUnderTest.start(testBuffer);

        // When
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:2021")
                        .method(HttpMethod.GET)
                        .path("/health")
                        .build())
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.UNAUTHORIZED)).join();
    }

    @Test
    public void testHTTPJsonResponse400() {
        // Prepare
        final String testBadData = "}";
        final int testPayloadSize = testBadData.getBytes().length;
        HTTPSourceUnderTest.start(testBuffer);
        refreshMeasurements();

        // When
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:2021")
                        .method(HttpMethod.POST)
                        .path("/log/ingest")
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

    @Test
    public void testHTTPJsonResponse413() throws InterruptedException {
        // Prepare
        final String testData = "[{\"log\": \"test log 1\"}, {\"log\": \"test log 2\"}]";
        final int testPayloadSize = testData.getBytes().length;
        HTTPSourceUnderTest.start(testBuffer);
        refreshMeasurements();

        // When
        WebClient.of().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTP)
                        .authority("127.0.0.1:2021")
                        .method(HttpMethod.POST)
                        .path("/log/ingest")
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

    @Test
    public void testHTTPJsonResponse408() {
        // Prepare
        final int testMaxPendingRequests = 1;
        final int testThreadCount = 1;
        final int serverTimeoutInMillis = 500;
        final int bufferTimeoutInMillis = 400;
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(serverTimeoutInMillis);
        when(sourceConfig.getBufferTimeoutInMillis()).thenReturn(bufferTimeoutInMillis);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(testMaxPendingRequests);
        when(sourceConfig.getThreadCount()).thenReturn(testThreadCount);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        // Start the source
        HTTPSourceUnderTest.start(testBuffer);
        refreshMeasurements();
        final RequestHeaders testRequestHeaders = RequestHeaders.builder().scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:2021")
                .method(HttpMethod.POST)
                .path("/log/ingest")
                .contentType(MediaType.JSON_UTF_8)
                .build();
        final HttpData testHttpData = HttpData.ofUtf8("[{\"log\": \"somelog\"}]");

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

    @Test
    public void testHTTPJsonResponse429() throws InterruptedException {
        // Prepare
        final int testMaxPendingRequests = 1;
        final int testThreadCount = 1;
        final int clientTimeoutInMillis = 100;
        final int serverTimeoutInMillis = (testMaxPendingRequests + testThreadCount + 1) * clientTimeoutInMillis;
        final Random rand = new Random();
        final double randomFactor = rand.nextDouble() + 1.5;
        final int requestTimeoutInMillis = (int)(serverTimeoutInMillis * randomFactor);
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(requestTimeoutInMillis);
        when(sourceConfig.getBufferTimeoutInMillis()).thenReturn(serverTimeoutInMillis);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(testMaxPendingRequests);
        when(sourceConfig.getThreadCount()).thenReturn(testThreadCount);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        // Start the source
        HTTPSourceUnderTest.start(testBuffer);
        refreshMeasurements();
        final RequestHeaders testRequestHeaders = RequestHeaders.builder().scheme(SessionProtocol.HTTP)
                .authority("127.0.0.1:2021")
                .method(HttpMethod.POST)
                .path("/log/ingest")
                .contentType(MediaType.JSON_UTF_8)
                .build();
        final HttpData testHttpData = HttpData.ofUtf8("[{\"log\": \"somelog\"}]");

        // Fill in the buffer
        WebClient.of().execute(testRequestHeaders, testHttpData).aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();

        // Send requests to throttle the server when buffer is full
        // Set the client timeout to be less than source serverTimeoutInMillis / (testMaxPendingRequests + testThreadCount)
        WebClient testWebClient = WebClient.builder().responseTimeoutMillis(clientTimeoutInMillis).build();
        for (int i = 0; i < testMaxPendingRequests + testThreadCount; i++) {
            CompletionException actualException = Assertions.assertThrows(
                    CompletionException.class, () -> testWebClient.execute(testRequestHeaders, testHttpData).aggregate().join());
            assertThat(actualException.getCause(), instanceOf(ResponseTimeoutException.class));
        }

        // When/Then
        testWebClient.execute(testRequestHeaders, testHttpData).aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.TOO_MANY_REQUESTS)).join();

        // Wait until source server timeout a request processing thread
        Thread.sleep(serverTimeoutInMillis);
        // New request should timeout instead of being rejected
        CompletionException actualException = Assertions.assertThrows(
                CompletionException.class, () -> testWebClient.execute(testRequestHeaders, testHttpData).aggregate().join());
        assertThat(actualException.getCause(), instanceOf(ResponseTimeoutException.class));
        // verify metrics
        final Measurement requestReceivedCount = MetricsTestUtil.getMeasurementFromList(
                requestsReceivedMeasurements, Statistic.COUNT);
        Assertions.assertEquals(testMaxPendingRequests + testThreadCount + 2, requestReceivedCount.getValue());
        final Measurement successRequestsCount = MetricsTestUtil.getMeasurementFromList(
                successRequestsMeasurements, Statistic.COUNT);
        Assertions.assertEquals(1.0, successRequestsCount.getValue());
        final Measurement rejectedRequestsCount = MetricsTestUtil.getMeasurementFromList(
                rejectedRequestsMeasurements, Statistic.COUNT);
        Assertions.assertEquals(1.0, rejectedRequestsCount.getValue());
    }

    @Test
    public void testServerStartCertFileSuccess() throws IOException {
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(server.stop()).thenReturn(completableFuture);

            final Path certFilePath = new File(TEST_SSL_CERTIFICATE_FILE).toPath();
            final Path keyFilePath = new File(TEST_SSL_KEY_FILE).toPath();
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);

            when(sourceConfig.isSsl()).thenReturn(true);
            when(sourceConfig.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
            when(sourceConfig.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);
            HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
            HTTPSourceUnderTest.start(testBuffer);
            HTTPSourceUnderTest.stop();

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
    void testHTTPSJsonResponse() {
        reset(sourceConfig);
        when(sourceConfig.getPort()).thenReturn(2021);
        when(sourceConfig.getPath()).thenReturn(HTTPSourceConfig.DEFAULT_LOG_INGEST_URI);
        when(sourceConfig.getThreadCount()).thenReturn(200);
        when(sourceConfig.getMaxConnectionCount()).thenReturn(500);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(1024);
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(200);
        when(sourceConfig.isSsl()).thenReturn(true);
        when(sourceConfig.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
        when(sourceConfig.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        testBuffer = getBuffer();
        HTTPSourceUnderTest.start(testBuffer);

        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                        .scheme(SessionProtocol.HTTPS)
                        .authority("127.0.0.1:2021")
                        .method(HttpMethod.POST)
                        .path("/log/ingest")
                        .contentType(MediaType.JSON_UTF_8)
                        .build(),
                HttpData.ofUtf8("[{\"log\": \"somelog\"}]"))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();
    }

    @Test
    void testHTTPSJsonResponse_with_custom_path_along_with_placeholder() {
        reset(sourceConfig);
        when(sourceConfig.getPort()).thenReturn(2021);
        when(sourceConfig.getPath()).thenReturn("/${pipelineName}/test");
        when(sourceConfig.getThreadCount()).thenReturn(200);
        when(sourceConfig.getMaxConnectionCount()).thenReturn(500);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(1024);
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(200);
        when(sourceConfig.isSsl()).thenReturn(true);

        when(sourceConfig.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
        when(sourceConfig.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);

        testBuffer = getBuffer();
        HTTPSourceUnderTest.start(testBuffer);

        final String path = "/" + TEST_PIPELINE_NAME + "/test";

        WebClient.builder().factory(ClientFactory.insecure()).build().execute(RequestHeaders.builder()
                                .scheme(SessionProtocol.HTTPS)
                                .authority("127.0.0.1:2021")
                                .method(HttpMethod.POST)
                                .path(path)
                                .contentType(MediaType.JSON_UTF_8)
                                .build(),
                        HttpData.ofUtf8("[{\"log\": \"somelog\"}]"))
                .aggregate()
                .whenComplete((i, ex) -> assertSecureResponseWithStatusCode(i, HttpStatus.OK)).join();
    }

    @Test
    public void testDoubleStart() {
        // starting server
        HTTPSourceUnderTest.start(testBuffer);
        // double start server
        Assertions.assertThrows(IllegalStateException.class, () -> HTTPSourceUnderTest.start(testBuffer));
    }

    @Test
    public void testStartWithEmptyBuffer() {
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        Assertions.assertThrows(IllegalStateException.class, () -> source.start(null));
    }

    @Test
    public void testStartWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));

            // When/Then
            Assertions.assertThrows(RuntimeException.class, () -> source.start(testBuffer));
        }
    }

    @Test
    public void testStartWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            final NullPointerException expCause = new NullPointerException();
            when(completableFuture.get()).thenThrow(new ExecutionException("", expCause));

            // When/Then
            final RuntimeException ex = Assertions.assertThrows(RuntimeException.class, () -> source.start(testBuffer));
            Assertions.assertEquals(expCause, ex);
        }
    }

    @Test
    public void testStartWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            Assertions.assertThrows(RuntimeException.class, () -> source.start(testBuffer));
            Assertions.assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(testBuffer);
            when(server.stop()).thenReturn(completableFuture);

            // When/Then
            when(completableFuture.get()).thenThrow(new ExecutionException("", null));
            Assertions.assertThrows(RuntimeException.class, source::stop);
        }
    }

    @Test
    public void testStopWithServerExecutionExceptionWithCause() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(testBuffer);
            when(server.stop()).thenReturn(completableFuture);
            final NullPointerException expCause = new NullPointerException();
            when(completableFuture.get()).thenThrow(new ExecutionException("", expCause));

            // When/Then
            final RuntimeException ex = Assertions.assertThrows(RuntimeException.class, source::stop);
            Assertions.assertEquals(expCause, ex);
        }
    }

    @Test
    public void testStopWithInterruptedException() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        try (MockedStatic<Server> armeriaServerMock = Mockito.mockStatic(Server.class)) {
            armeriaServerMock.when(Server::builder).thenReturn(serverBuilder);
            source.start(testBuffer);
            when(server.stop()).thenReturn(completableFuture);
            when(completableFuture.get()).thenThrow(new InterruptedException());

            // When/Then
            Assertions.assertThrows(RuntimeException.class, source::stop);
            Assertions.assertTrue(Thread.interrupted());
        }
    }

    @Test
    public void testRunAnotherSourceWithSamePort() {
        // starting server
        HTTPSourceUnderTest.start(testBuffer);

        final HTTPSource secondSource = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory, pipelineDescription);
        //Expect RuntimeException because when port is already in use, BindException is thrown which is not RuntimeException
        Assertions.assertThrows(RuntimeException.class, () -> secondSource.start(testBuffer));
    }
}
