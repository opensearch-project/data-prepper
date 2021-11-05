/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.source.loghttp;

import com.amazon.dataprepper.armeria.authentication.ArmeriaAuthenticationProvider;
import com.amazon.dataprepper.metrics.MetricNames;
import com.amazon.dataprepper.metrics.MetricsTestUtil;
import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.plugin.PluginFactory;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import com.linecorp.armeria.client.ClientFactory;
import com.linecorp.armeria.client.ResponseTimeoutException;
import com.linecorp.armeria.client.WebClient;
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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
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

    private BlockingBuffer<Record<String>> testBuffer;
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

    private BlockingBuffer<Record<String>> getBuffer() {
        final HashMap<String, Object> integerHashMap = new HashMap<>();
        integerHashMap.put("buffer_size", 1);
        integerHashMap.put("batch_size", 1);
        final PluginSetting pluginSetting = new PluginSetting("blocking_buffer", integerHashMap) {{
            setPipelineName(TEST_PIPELINE_NAME);
        }};
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
                        .add(RequestExceptionHandler.REQUEST_TIMEOUTS).toString());
        badRequestsMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(RequestExceptionHandler.BAD_REQUESTS).toString());
        requestsTooLargeMeasurements = MetricsTestUtil.getMeasurementList(
                new StringJoiner(MetricNames.DELIMITER).add(metricNamePrefix)
                        .add(RequestExceptionHandler.REQUESTS_TOO_LARGE).toString());
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

    @BeforeEach
    public void setUp() {
        lenient().when(serverBuilder.annotatedService(any())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.http(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.https(anyInt())).thenReturn(serverBuilder);
        lenient().when(serverBuilder.build()).thenReturn(server);
        lenient().when(server.start()).thenReturn(completableFuture);

        sourceConfig = mock(HTTPSourceConfig.class);
        lenient().when(sourceConfig.getPort()).thenReturn(2021);
        lenient().when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(10_000);
        lenient().when(sourceConfig.getThreadCount()).thenReturn(200);
        lenient().when(sourceConfig.getMaxConnectionCount()).thenReturn(500);
        lenient().when(sourceConfig.getMaxPendingRequests()).thenReturn(1024);

        MetricsTestUtil.initMetrics();
        pluginMetrics = PluginMetrics.fromNames(PLUGIN_NAME, TEST_PIPELINE_NAME);

        pluginFactory = mock(PluginFactory.class);
        final ArmeriaAuthenticationProvider authenticationProvider = mock(ArmeriaAuthenticationProvider.class);
        when(pluginFactory.loadPlugin(eq(ArmeriaAuthenticationProvider.class), any(PluginSetting.class)))
                .thenReturn(authenticationProvider);

        testBuffer = getBuffer();
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory);
    }

    @AfterEach
    public void cleanUp() {
        if (HTTPSourceUnderTest != null) {
            HTTPSourceUnderTest.stop();
        }
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
                .whenComplete((i, ex) -> assertThat(i.status()).isEqualTo(HttpStatus.OK)).join();

        // Then
        Assertions.assertFalse(testBuffer.isEmpty());
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
                .whenComplete((i, ex) -> assertThat(i.status()).isEqualTo(HttpStatus.BAD_REQUEST)).join();

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
                .whenComplete((i, ex) -> assertThat(i.status()).isEqualTo(HttpStatus.REQUEST_ENTITY_TOO_LARGE)).join();

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
    public void testHTTPJsonResponse415() {
        // Prepare
        final int testMaxPendingRequests = 1;
        final int testThreadCount = 1;
        final int serverTimeoutInMillis = 500;
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(serverTimeoutInMillis);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(testMaxPendingRequests);
        when(sourceConfig.getThreadCount()).thenReturn(testThreadCount);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory);
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
        WebClient.of().execute(testRequestHeaders, testHttpData).aggregate().whenComplete(
                (response, ex) -> assertThat(response.status()).isEqualTo(HttpStatus.OK)).join();

        // Disable client timeout
        WebClient testWebClient = WebClient.builder().responseTimeoutMillis(0).build();

        // When/Then
        testWebClient.execute(testRequestHeaders, testHttpData)
                .aggregate()
                .whenComplete((i, ex) -> assertThat(i.status()).isEqualTo(HttpStatus.REQUEST_TIMEOUT))
                .join();
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
        Assertions.assertTrue(maxDurationInMillis > serverTimeoutInMillis);
    }

    @Test
    public void testHTTPJsonResponse429() throws InterruptedException {
        // Prepare
        final Map<String, Object> settings = new HashMap<>();
        final int testMaxPendingRequests = 1;
        final int testThreadCount = 1;
        final int clientTimeoutInMillis = 100;
        final int serverTimeoutInMillis = (testMaxPendingRequests + testThreadCount + 1) * clientTimeoutInMillis;
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(serverTimeoutInMillis);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(testMaxPendingRequests);
        when(sourceConfig.getThreadCount()).thenReturn(testThreadCount);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory);
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
        WebClient.of().execute(testRequestHeaders, testHttpData).aggregate().whenComplete(
                (response, ex) -> assertThat(response.status()).isEqualTo(HttpStatus.OK)).join();

        // Send requests to throttle the server when buffer is full
        // Set the client timeout to be less than source serverTimeoutInMillis / (testMaxPendingRequests + testThreadCount)
        WebClient testWebClient = WebClient.builder().responseTimeoutMillis(clientTimeoutInMillis).build();
        for (int i = 0; i < testMaxPendingRequests + testThreadCount; i++) {
            CompletionException actualException = Assertions.assertThrows(
                    CompletionException.class, () -> testWebClient.execute(testRequestHeaders, testHttpData).aggregate().join());
            assertThat(actualException.getCause()).isInstanceOf(ResponseTimeoutException.class);
        }

        // When/Then
        testWebClient.execute(testRequestHeaders, testHttpData).aggregate().whenComplete(
                (response, ex) -> assertThat(response.status()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS)).join();

        // Wait until source server timeout a request processing thread
        Thread.sleep(serverTimeoutInMillis);
        // New request should timeout instead of being rejected
        CompletionException actualException = Assertions.assertThrows(
                CompletionException.class, () -> testWebClient.execute(testRequestHeaders, testHttpData).aggregate().join());
        assertThat(actualException.getCause()).isInstanceOf(ResponseTimeoutException.class);
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

            final Path certFilePath = Path.of(TEST_SSL_CERTIFICATE_FILE);
            final Path keyFilePath = Path.of(TEST_SSL_KEY_FILE);
            final String certAsString = Files.readString(certFilePath);
            final String keyAsString = Files.readString(keyFilePath);

            when(sourceConfig.isSsl()).thenReturn(true);
            when(sourceConfig.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
            when(sourceConfig.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);
            HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory);
            HTTPSourceUnderTest.start(testBuffer);
            HTTPSourceUnderTest.stop();

            final ArgumentCaptor<InputStream> certificateIs = ArgumentCaptor.forClass(InputStream.class);
            final ArgumentCaptor<InputStream> privateKeyIs = ArgumentCaptor.forClass(InputStream.class);
            verify(serverBuilder).tls(certificateIs.capture(), privateKeyIs.capture());
            final String actualCertificate = IOUtils.toString(certificateIs.getValue(), StandardCharsets.UTF_8.name());
            final String actualPrivateKey = IOUtils.toString(privateKeyIs.getValue(), StandardCharsets.UTF_8.name());
            assertThat(actualCertificate).isEqualTo(certAsString);
            assertThat(actualPrivateKey).isEqualTo(keyAsString);
        }
    }

    @Test
    void testHTTPSJsonResponse() {
        reset(sourceConfig);
        when(sourceConfig.getPort()).thenReturn(2021);
        when(sourceConfig.getThreadCount()).thenReturn(200);
        when(sourceConfig.getMaxConnectionCount()).thenReturn(500);
        when(sourceConfig.getMaxPendingRequests()).thenReturn(1024);
        when(sourceConfig.getRequestTimeoutInMillis()).thenReturn(200);
        when(sourceConfig.isSsl()).thenReturn(true);
        when(sourceConfig.getSslCertificateFile()).thenReturn(TEST_SSL_CERTIFICATE_FILE);
        when(sourceConfig.getSslKeyFile()).thenReturn(TEST_SSL_KEY_FILE);
        HTTPSourceUnderTest = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory);

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
                .whenComplete((i, ex) -> assertThat(i.status().code()).isEqualTo(200)).join();
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
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory);
        Assertions.assertThrows(IllegalStateException.class, () -> source.start(null));
    }

    @Test
    public void testStartWithServerExecutionExceptionNoCause() throws ExecutionException, InterruptedException {
        // Prepare
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory);
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
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory);
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
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory);
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
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory);
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
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory);
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
        final HTTPSource source = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory);
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

        final HTTPSource secondSource = new HTTPSource(sourceConfig, pluginMetrics, pluginFactory);
        //Expect RuntimeException because when port is already in use, BindException is thrown which is not RuntimeException
        Assertions.assertThrows(RuntimeException.class, () -> secondSource.start(testBuffer));
    }
}