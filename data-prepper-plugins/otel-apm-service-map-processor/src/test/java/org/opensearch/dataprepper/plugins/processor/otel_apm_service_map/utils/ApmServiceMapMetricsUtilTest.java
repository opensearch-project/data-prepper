/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.apache.commons.codec.binary.Hex;
import org.opensearch.dataprepper.model.metric.Exemplar;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonMetric;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.ClientSpanDecoration;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.HistogramBuckets;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.MetricAggregationState;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.MetricKey;
import org.opensearch.dataprepper.plugins.processor.otel_apm_service_map.model.internal.SpanStateData;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class ApmServiceMapMetricsUtilTest {

    private String testHostId;

    private SpanStateData testClientSpan;
    private SpanStateData testServerSpan;
    private ClientSpanDecoration mockDecoration;
    private Map<MetricKey, MetricAggregationState> sumStateByKey;
    private Map<MetricKey, MetricAggregationState> histogramStateByKey;
    private Instant currentTime;
    private Instant anchorTimestamp;

    @BeforeEach
    void setUp() {
        testHostId = java.util.UUID.randomUUID().toString();
        testClientSpan = createMockSpanStateData("client-service", "client-operation", "test-env");
        testServerSpan = createMockSpanStateData("server-service", "server-operation", "test-env");
        mockDecoration = createMockClientSpanDecoration();
        sumStateByKey = new HashMap<>();
        histogramStateByKey = new HashMap<>();
        currentTime = Instant.now();
        anchorTimestamp = Instant.now().minusSeconds(60).truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
    }

    private SpanStateData createMockSpanStateData(String serviceName, String operationName, String environment) {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("resource", Map.of("attributes", Map.of("deployment.environment.name", environment)));

        return new SpanStateData(
                serviceName,
                Hex.encodeHexString(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}),
                Hex.encodeHexString(new byte[]{9, 10, 11, 12, 13, 14, 15, 16}),
                Hex.encodeHexString(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
                "SERVER",
                operationName,
                operationName,
                1000000000L, // 1 second in nanos
                "OK",
                "2023-01-01T00:00:00.000Z",
                Collections.singletonMap("custom", "value"),
                spanAttributes
        );
    }

    private ClientSpanDecoration createMockClientSpanDecoration() {
        return new ClientSpanDecoration(
                "parent-server-op",
                "remote-env",
                "remote-service",
                "remote-operation",
                Collections.emptyMap()
        );
    }

    private SpanStateData createSpanWithHttpStatus(int httpStatusCode) {
        return createSpanWithHttpStatus(httpStatusCode, "test-service", "test-operation", "test-env");
    }

    private SpanStateData createSpanWithHttpStatus(int httpStatusCode, String serviceName, String operationName, String environment) {
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("http.response.status_code", httpStatusCode);
        spanAttributes.put("resource", Map.of("attributes", Map.of("deployment.environment.name", environment)));

        return new SpanStateData(
                serviceName,
                Hex.encodeHexString(new byte[]{1, 2, 3, 4, 5, 6, 7, 8}),
                Hex.encodeHexString(new byte[]{9, 10, 11, 12, 13, 14, 15, 16}),
                Hex.encodeHexString(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}),
                "SERVER",
                operationName,
                operationName,
                1000000000L, // 1 second in nanos
                "OK",
                "2023-01-01T00:00:00.000Z",
                Collections.singletonMap("custom", "value"),
                spanAttributes
        );
    }

    @Test
    void testGenerateMetricsForClientSpan_Success() {
        // When
        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                testClientSpan, mockDecoration, currentTime,
                sumStateByKey, histogramStateByKey,
                anchorTimestamp, testHostId);

        // Then
        assertEquals(1, sumStateByKey.size());
        MetricAggregationState sumState = sumStateByKey.values().iterator().next();
        assertEquals(1, sumState.getRequestCount());
        assertEquals(0, sumState.getErrorCount());
        assertEquals(0, sumState.getFaultCount());

        assertEquals(1, histogramStateByKey.size());
        MetricAggregationState histState = histogramStateByKey.values().iterator().next();
        assertEquals(1, histState.getLatencyDurations().size());
        assertEquals(1.0, histState.getLatencyDurations().get(0), 0.001);
    }

    @Test
    void testGenerateMetricsForClientSpan_WithError() {
        // Given - Create span with error status
        SpanStateData errorSpan = createSpanWithHttpStatus(400); // HTTP 400 = error

        // When
        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                errorSpan, mockDecoration, currentTime,
                sumStateByKey, histogramStateByKey,
                anchorTimestamp, testHostId);

        // Then
        MetricAggregationState sumState = sumStateByKey.values().iterator().next();
        assertEquals(1, sumState.getRequestCount());
        assertEquals(1, sumState.getErrorCount());
        assertEquals(0, sumState.getFaultCount());
        assertEquals(1, sumState.getErrorExemplars().size());
        assertEquals(0, sumState.getFaultExemplars().size());
    }

    @Test
    void testGenerateMetricsForClientSpan_WithFault() {
        // Given - Create span with fault status
        SpanStateData faultSpan = createSpanWithHttpStatus(500); // HTTP 500 = fault

        // When
        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                faultSpan, mockDecoration, currentTime,
                sumStateByKey, histogramStateByKey,
                anchorTimestamp, testHostId);

        // Then
        MetricAggregationState sumState = sumStateByKey.values().iterator().next();
        assertEquals(1, sumState.getRequestCount());
        assertEquals(0, sumState.getErrorCount());
        assertEquals(1, sumState.getFaultCount());
        assertEquals(0, sumState.getErrorExemplars().size());
        assertEquals(1, sumState.getFaultExemplars().size());
    }

    @Test
    void testGenerateMetricsForClientSpan_WithNullDuration() {
        // Given
        testClientSpan.setDurationInNanos(null);

        // When
        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                testClientSpan, mockDecoration, currentTime,
                sumStateByKey, histogramStateByKey,
                anchorTimestamp, testHostId);

        // Then
        MetricAggregationState sumState = sumStateByKey.values().iterator().next();
        assertEquals(1, sumState.getRequestCount());
        assertTrue(histogramStateByKey.isEmpty()); // No histogram entry for null duration
    }

    @Test
    void testGenerateMetricsForClientSpan_WithZeroDuration() {
        // Given
        testClientSpan.setDurationInNanos(0L);

        // When
        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                testClientSpan, mockDecoration, currentTime,
                sumStateByKey, histogramStateByKey,
                anchorTimestamp, testHostId);

        // Then
        MetricAggregationState sumState = sumStateByKey.values().iterator().next();
        assertEquals(1, sumState.getRequestCount());
        assertTrue(histogramStateByKey.isEmpty()); // No histogram entry for zero duration
    }

    @Test
    void testGenerateMetricsForClientSpan_ExemplarLimit() {
        // Given - Create span with error status
        SpanStateData errorSpan = createSpanWithHttpStatus(400);
        MetricAggregationState existingState = new MetricAggregationState();
        // Pre-fill with 10 exemplars
        for (int i = 0; i < 10; i++) {
            existingState.addErrorExemplar(mock(Exemplar.class));
        }

        Map<String, Object> labels = new HashMap<>();
        labels.put("namespace", "span_derived");
        labels.put("environment", errorSpan.getEnvironment());
        labels.put("service", errorSpan.getServiceName());
        labels.put("operation", mockDecoration.getParentServerOperationName());
        labels.put("remoteEnvironment", mockDecoration.getRemoteEnvironment());
        labels.put("remoteService", mockDecoration.getRemoteService());
        labels.put("remoteOperation", mockDecoration.getRemoteOperation());
        labels.put("service_map_processor_host_id", testHostId);
        labels.putAll(errorSpan.getGroupByAttributes());

        MetricKey key = new MetricKey(labels, anchorTimestamp);
        sumStateByKey.put(key, existingState);

        // When
        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                errorSpan, mockDecoration, currentTime,
                sumStateByKey, histogramStateByKey,
                anchorTimestamp, testHostId);

        // Then
        assertEquals(10, existingState.getErrorExemplars().size()); // Should not exceed limit
    }

    @Test
    void testGenerateMetricsForServerSpan_Success() {
        // When
        ApmServiceMapMetricsUtil.generateMetricsForServerSpan(
                testServerSpan, currentTime,
                sumStateByKey, histogramStateByKey,
                anchorTimestamp, testHostId);

        // Then
        assertEquals(1, sumStateByKey.size());
        MetricAggregationState sumState = sumStateByKey.values().iterator().next();
        assertEquals(1, sumState.getRequestCount());
        assertEquals(0, sumState.getErrorCount());
        assertEquals(0, sumState.getFaultCount());

        assertEquals(1, histogramStateByKey.size());
        MetricAggregationState histState = histogramStateByKey.values().iterator().next();
        assertEquals(1, histState.getLatencyDurations().size());
    }

    @Test
    void testGenerateMetricsForServerSpan_WithError() {
        // Given - Create span with error status
        SpanStateData errorSpan = createSpanWithHttpStatus(400); // HTTP 400 = error

        // When
        ApmServiceMapMetricsUtil.generateMetricsForServerSpan(
                errorSpan, currentTime,
                sumStateByKey, histogramStateByKey,
                anchorTimestamp, testHostId);

        // Then
        MetricAggregationState sumState = sumStateByKey.values().iterator().next();
        assertEquals(1, sumState.getRequestCount());
        assertEquals(1, sumState.getErrorCount());
        assertEquals(0, sumState.getFaultCount());
        assertEquals(1, sumState.getErrorExemplars().size());
        assertEquals(0, sumState.getFaultExemplars().size());
    }

    @Test
    void testGenerateMetricsForServerSpan_WithFault() {
        // Given - Create span with fault status
        SpanStateData faultSpan = createSpanWithHttpStatus(500); // HTTP 500 = fault

        // When
        ApmServiceMapMetricsUtil.generateMetricsForServerSpan(
                faultSpan, currentTime,
                sumStateByKey, histogramStateByKey,
                anchorTimestamp, testHostId);

        // Then
        MetricAggregationState sumState = sumStateByKey.values().iterator().next();
        assertEquals(1, sumState.getRequestCount());
        assertEquals(0, sumState.getErrorCount());
        assertEquals(1, sumState.getFaultCount());
        assertEquals(0, sumState.getErrorExemplars().size());
        assertEquals(1, sumState.getFaultExemplars().size());
    }

    @Test
    void testCreateMetricsFromAggregatedState_EmptyLatencyDurations() {
        // Given
        MetricAggregationState sumState = new MetricAggregationState(1, 0, 0);

        Map<String, Object> labels = new HashMap<>();
        labels.put("service", "test-service");

        MetricKey key = new MetricKey(labels, anchorTimestamp);
        sumStateByKey.put(key, sumState);
        // histogramStateByKey is empty

        // When
        List<JacksonMetric> metrics = ApmServiceMapMetricsUtil.createMetricsFromAggregatedState(
                sumStateByKey, histogramStateByKey);

        // Then
        assertEquals(3, metrics.size()); // Only request, error, fault (no latency)
    }

    @Test
    void testCreateMetricsFromAggregatedState_WithBothSumAndHistogram() {
        // Given
        MetricAggregationState sumState = new MetricAggregationState(5, 2, 1);
        Map<String, Object> labels = new HashMap<>();
        labels.put("service", "test-service");
        sumStateByKey.put(new MetricKey(labels, anchorTimestamp), sumState);

        MetricAggregationState histState = new MetricAggregationState();
        histState.addLatencyDuration(0.1);
        histState.addLatencyDuration(0.5);
        histogramStateByKey.put(new MetricKey(labels, anchorTimestamp), histState);

        // When
        List<JacksonMetric> metrics = ApmServiceMapMetricsUtil.createMetricsFromAggregatedState(
                sumStateByKey, histogramStateByKey);

        // Then
        assertEquals(4, metrics.size()); // request, error, fault, latency

        List<String> metricNames = metrics.stream()
                .map(JacksonMetric::getName)
                .collect(Collectors.toList());
        assertTrue(metricNames.contains("request"));
        assertTrue(metricNames.contains("error"));
        assertTrue(metricNames.contains("fault"));
        assertTrue(metricNames.contains("latency"));
    }

    @Test
    void testCreateExemplarFromSpan_Success() {
        // When
        Exemplar exemplar = ApmServiceMapMetricsUtil.createExemplarFromSpan(testClientSpan, 1.0);

        // Then
        assertNotNull(exemplar);
        assertEquals(1.0, exemplar.getValue());
        assertNotNull(exemplar.getAttributes());
        assertTrue(exemplar.getAttributes().containsKey("service.name"));
        assertTrue(exemplar.getAttributes().containsKey("operation.name"));
    }

    @Test
    void testCreateExemplarFromSpan_WithException() {
        // Given - Create a corrupted span that will cause issues
        SpanStateData corruptedSpan = new SpanStateData(
                null, null, null, null,
                "SERVER", "test-op", "test-op",
                1000000000L, "OK", "2023-01-01T00:00:00.000Z",
                Collections.emptyMap(), Collections.emptyMap()
        );

        // When
        Exemplar exemplar = ApmServiceMapMetricsUtil.createExemplarFromSpan(corruptedSpan, 1.0);

        // Then
        assertNotNull(exemplar);
        assertEquals(1.0, exemplar.getValue());
    }

    @Test
    void testCreateExemplarFromSpan_WithNullStatus() {
        // Given
        testClientSpan.setStatus(null);

        // When
        Exemplar exemplar = ApmServiceMapMetricsUtil.createExemplarFromSpan(testClientSpan, 1.0);

        // Then
        assertNotNull(exemplar);
        assertEquals(1.0, exemplar.getValue());
        assertFalse(exemplar.getAttributes().containsKey("status"));
    }

    @Test
    void testCreateJacksonSumMetric_Success() {
        // Given
        String metricName = "test_metric";
        String description = "Test metric description";
        double value = 10.0;
        Map<String, Object> labels = new HashMap<>();
        labels.put("service", "test-service");
        List<Exemplar> exemplars = Collections.emptyList();

        // When
        JacksonMetric metric = ApmServiceMapMetricsUtil.createJacksonSumMetric(
                metricName, description, value, labels, anchorTimestamp, exemplars);

        // Then
        assertNotNull(metric);
        assertInstanceOf(JacksonSum.class, metric);
        assertEquals(metricName, metric.getName());
        assertEquals(description, metric.getDescription());
        assertNotNull(metric.getAttributes());
        assertFalse(metric.getAttributes().containsKey("randomKey"));
    }

    @Test
    void testCreateJacksonStandardHistogram_Success() {
        // Given
        String metricName = "latency_histogram";
        String description = "Latency histogram";
        List<Double> durations = Arrays.asList(0.1, 0.5, 1.0, 2.0);
        Map<String, Object> labels = new HashMap<>();
        labels.put("service", "test-service");

        // When
        JacksonMetric metric = ApmServiceMapMetricsUtil.createJacksonStandardHistogram(
                metricName, description, durations, labels, anchorTimestamp);

        // Then
        assertNotNull(metric);
        assertEquals(metricName, metric.getName());
        assertEquals(description, metric.getDescription());
        assertNotNull(metric.getAttributes());

        if (metric instanceof JacksonHistogram) {
            JacksonHistogram histogram = (JacksonHistogram) metric;
            assertEquals(4L, histogram.getCount());
            assertEquals(3.6, histogram.getSum(), 0.001);
            assertEquals(0.1, histogram.getMin(), 0.001);
            assertEquals(2.0, histogram.getMax(), 0.001);
            assertNotNull(histogram.getBucketCountsList());
            assertNotNull(histogram.getExplicitBoundsList());
        } else {
            fail("Expected JacksonHistogram but got: " + metric.getClass().getSimpleName());
        }
    }

    @Test
    void testCreateHistogramBucketsFromDurations_Success() {
        // Given
        List<Double> durations = Arrays.asList(0.001, 0.01, 0.1, 1.0, 5.0, 15.0);

        // When
        HistogramBuckets buckets = ApmServiceMapMetricsUtil.createHistogramBucketsFromDurations(durations);

        // Then
        assertNotNull(buckets);
        assertNotNull(buckets.getBucketCounts());
        assertNotNull(buckets.getExplicitBounds());
        assertEquals(16, buckets.getBucketCounts().size());
        assertEquals(15, buckets.getExplicitBounds().size());

        long totalCount = buckets.getBucketCounts().stream().mapToLong(Long::longValue).sum();
        assertEquals(durations.size(), totalCount);
    }

    @Test
    void testCreateHistogramBucketsFromDurations_BoundaryValues() {
        // Given
        List<Double> durations = Arrays.asList(0.0, 0.005, 0.01, 0.025);

        // When
        HistogramBuckets buckets = ApmServiceMapMetricsUtil.createHistogramBucketsFromDurations(durations);

        // Then
        assertNotNull(buckets);
        long totalCount = buckets.getBucketCounts().stream().mapToLong(Long::longValue).sum();
        assertEquals(4, totalCount);

        boolean hasBucketData = buckets.getBucketCounts().stream().anyMatch(count -> count > 0);
        assertTrue(hasBucketData, "At least some buckets should contain data");
    }

    @Test
    void testCreateHistogramBucketsFromDurations_WithNullValues() {
        // Given
        List<Double> durations = new ArrayList<>();
        durations.add(0.1);
        durations.add(null);
        durations.add(1.0);

        // When
        HistogramBuckets buckets = ApmServiceMapMetricsUtil.createHistogramBucketsFromDurations(durations);

        // Then
        assertNotNull(buckets);
        long totalCount = buckets.getBucketCounts().stream().mapToLong(Long::longValue).sum();
        assertEquals(2, totalCount);
    }

    @Test
    void testCreateHistogramBucketsFromDurations_EmptyList() {
        // Given
        List<Double> durations = Collections.emptyList();

        // When
        HistogramBuckets buckets = ApmServiceMapMetricsUtil.createHistogramBucketsFromDurations(durations);

        // Then
        assertNotNull(buckets);
        assertEquals(16, buckets.getBucketCounts().size());
        assertEquals(15, buckets.getExplicitBounds().size());
        for (Long count : buckets.getBucketCounts()) {
            assertEquals(0L, count);
        }
    }

    @Test
    void testCreateHistogramBucketsFromDurations_OverflowBucket() {
        // Given
        List<Double> durations = Arrays.asList(20.0, 100.0);

        // When
        HistogramBuckets buckets = ApmServiceMapMetricsUtil.createHistogramBucketsFromDurations(durations);

        // Then
        assertNotNull(buckets);
        assertEquals(2L, buckets.getBucketCounts().get(buckets.getBucketCounts().size() - 1));
        for (int i = 0; i < buckets.getBucketCounts().size() - 1; i++) {
            assertEquals(0L, buckets.getBucketCounts().get(i));
        }
    }

    @Test
    void testMultipleSpansAggregation() {
        // Given
        SpanStateData span1 = createSpanWithHttpStatus(400, "service1", "op1", "env1"); // Error
        SpanStateData span2 = createSpanWithHttpStatus(500, "service1", "op1", "env1"); // Fault
        span1.setDurationInNanos(1000000000L); // 1 second
        span2.setDurationInNanos(2000000000L); // 2 seconds

        // When
        ApmServiceMapMetricsUtil.generateMetricsForServerSpan(
                span1, currentTime, sumStateByKey, histogramStateByKey,
                anchorTimestamp, testHostId);
        ApmServiceMapMetricsUtil.generateMetricsForServerSpan(
                span2, currentTime, sumStateByKey, histogramStateByKey,
                anchorTimestamp, testHostId);

        // Then - sum metrics aggregate by seconds
        assertEquals(1, sumStateByKey.size());
        MetricAggregationState sumState = sumStateByKey.values().iterator().next();
        assertEquals(2, sumState.getRequestCount());
        assertEquals(1, sumState.getErrorCount());
        assertEquals(1, sumState.getFaultCount());

        // Histogram metrics aggregate by minutes
        assertEquals(1, histogramStateByKey.size());
        MetricAggregationState histState = histogramStateByKey.values().iterator().next();
        assertEquals(2, histState.getLatencyDurations().size());
        assertEquals(1.0, histState.getLatencyDurations().get(0), 0.001);
        assertEquals(2.0, histState.getLatencyDurations().get(1), 0.001);
    }

    @Test
    void testMetricsLabelsCorrectness_ClientSpan() {
        // When
        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                testClientSpan, mockDecoration, currentTime,
                sumStateByKey, histogramStateByKey,
                anchorTimestamp, testHostId);

        // Then
        MetricKey key = sumStateByKey.keySet().iterator().next();
        Map<String, Object> labels = key.getLabels();

        assertEquals("span_derived", labels.get("namespace"));
        assertEquals(testClientSpan.getEnvironment(), labels.get("environment"));
        assertEquals(testClientSpan.getServiceName(), labels.get("service"));
        assertEquals(mockDecoration.getParentServerOperationName(), labels.get("operation"));
        assertEquals(mockDecoration.getRemoteEnvironment(), labels.get("remoteEnvironment"));
        assertEquals(mockDecoration.getRemoteService(), labels.get("remoteService"));
        assertEquals(mockDecoration.getRemoteOperation(), labels.get("remoteOperation"));
        assertEquals(testHostId, labels.get("service_map_processor_host_id"));
        assertEquals("value", labels.get("custom"));
    }

    @Test
    void testMetricsLabelsCorrectness_ServerSpan() {
        // When
        ApmServiceMapMetricsUtil.generateMetricsForServerSpan(
                testServerSpan, currentTime,
                sumStateByKey, histogramStateByKey,
                anchorTimestamp, testHostId);

        // Then
        MetricKey key = sumStateByKey.keySet().iterator().next();
        Map<String, Object> labels = key.getLabels();

        assertEquals("span_derived", labels.get("namespace"));
        assertEquals(testServerSpan.getEnvironment(), labels.get("environment"));
        assertEquals(testServerSpan.getServiceName(), labels.get("service"));
        assertEquals(testServerSpan.getOperationName(), labels.get("operation"));
        assertEquals(testHostId, labels.get("service_map_processor_host_id"));
        assertEquals("value", labels.get("custom"));

        assertFalse(labels.containsKey("remoteEnvironment"));
        assertFalse(labels.containsKey("remoteService"));
        assertFalse(labels.containsKey("remoteOperation"));
    }

    @Test
    void testSumAndHistogramUseSameTimestamp() {
        // Given
        Instant anchor = Instant.parse("2023-01-01T00:01:30Z");

        // When
        ApmServiceMapMetricsUtil.generateMetricsForServerSpan(
                testServerSpan, currentTime,
                sumStateByKey, histogramStateByKey,
                anchor, testHostId);

        // Then
        MetricKey sumKey = sumStateByKey.keySet().iterator().next();
        MetricKey histKey = histogramStateByKey.keySet().iterator().next();

        assertEquals(anchor, sumKey.getTimestamp());
        assertEquals(anchor, histKey.getTimestamp());
    }

    @Test
    void testMetricsSortedByTimestamp() {
        // Given
        MetricAggregationState state1 = new MetricAggregationState(1, 0, 0);
        MetricAggregationState state2 = new MetricAggregationState(2, 0, 0);

        MetricAggregationState histState = new MetricAggregationState();
        histState.addLatencyDuration(1.0);

        Instant earlierTime = anchorTimestamp.minusSeconds(60);
        Instant laterTime = anchorTimestamp.plusSeconds(60);

        Map<String, Object> labels1 = new HashMap<>();
        labels1.put("service", "service1");

        Map<String, Object> labels2 = new HashMap<>();
        labels2.put("service", "service2");

        sumStateByKey.put(new MetricKey(labels2, laterTime), state2);
        sumStateByKey.put(new MetricKey(labels1, earlierTime), state1);
        histogramStateByKey.put(new MetricKey(labels1, earlierTime), histState);

        // When
        List<JacksonMetric> metrics = ApmServiceMapMetricsUtil.createMetricsFromAggregatedState(
                sumStateByKey, histogramStateByKey);

        // Then
        assertFalse(metrics.isEmpty());
        if (metrics.size() >= 2) {
            String firstTimestamp = metrics.get(0).getTime();
            String secondTimestamp = metrics.get(1).getTime();
            assertThat("Metrics should be sorted by timestamp",
                    firstTimestamp, lessThanOrEqualTo(secondTimestamp));
        }
    }
}
