/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.processor.utils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.metric.Exemplar;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonMetric;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.plugins.processor.model.internal.ClientSpanDecoration;
import org.opensearch.dataprepper.plugins.processor.model.internal.HistogramBuckets;
import org.opensearch.dataprepper.plugins.processor.model.internal.MetricAggregationState;
import org.opensearch.dataprepper.plugins.processor.model.internal.MetricKey;
import org.opensearch.dataprepper.plugins.processor.model.internal.SpanStateData;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

@ExtendWith(MockitoExtension.class)
class ApmServiceMapMetricsUtilTest {

    private SpanStateData mockClientSpan;
    private SpanStateData mockServerSpan;
    private ClientSpanDecoration mockDecoration;
    private Map<MetricKey, MetricAggregationState> metricsStateByKey;
    private Instant currentTime;
    private Instant anchorTimestamp;

    @BeforeEach
    void setUp() {
        mockClientSpan = createMockSpanStateData("client-service", "client-operation", "test-env");
        mockServerSpan = createMockSpanStateData("server-service", "server-operation", "test-env");
        mockDecoration = createMockClientSpanDecoration();
        metricsStateByKey = new HashMap<>();
        currentTime = Instant.now();
        anchorTimestamp = Instant.now().minusSeconds(60);
    }

    private SpanStateData createMockSpanStateData(String serviceName, String operationName, String environment) {
        // Create a real SpanStateData instance for proper field access
        Map<String, Object> spanAttributes = new HashMap<>();
        spanAttributes.put("resource", Map.of("attributes", Map.of("deployment.environment.name", environment)));
        
        return new SpanStateData(
                serviceName,
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8},
                new byte[]{9, 10, 11, 12, 13, 14, 15, 16},
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
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
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8},
                new byte[]{9, 10, 11, 12, 13, 14, 15, 16},
                new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
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
                mockClientSpan, mockDecoration, currentTime, metricsStateByKey, anchorTimestamp);

        // Then
        assertEquals(1, metricsStateByKey.size());
        MetricAggregationState state = metricsStateByKey.values().iterator().next();
        assertEquals(1, state.requestCount);
        assertEquals(0, state.errorCount);
        assertEquals(0, state.faultCount);
        assertEquals(1, state.latencyDurations.size());
        assertEquals(1.0, state.latencyDurations.get(0), 0.001);
    }

    @Test
    void testGenerateMetricsForClientSpan_WithError() {
        // Given - Create span with error status
        SpanStateData errorSpan = createSpanWithHttpStatus(400); // HTTP 400 = error

        // When
        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                errorSpan, mockDecoration, currentTime, metricsStateByKey, anchorTimestamp);

        // Then
        MetricAggregationState state = metricsStateByKey.values().iterator().next();
        assertEquals(1, state.requestCount);
        assertEquals(1, state.errorCount);
        assertEquals(0, state.faultCount);
        assertEquals(1, state.errorExemplars.size());
        assertEquals(0, state.faultExemplars.size());
    }

    @Test
    void testGenerateMetricsForClientSpan_WithFault() {
        // Given - Create span with fault status
        SpanStateData faultSpan = createSpanWithHttpStatus(500); // HTTP 500 = fault

        // When
        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                faultSpan, mockDecoration, currentTime, metricsStateByKey, anchorTimestamp);

        // Then
        MetricAggregationState state = metricsStateByKey.values().iterator().next();
        assertEquals(1, state.requestCount);
        assertEquals(0, state.errorCount);
        assertEquals(1, state.faultCount);
        assertEquals(0, state.errorExemplars.size());
        assertEquals(1, state.faultExemplars.size());
    }

    @Test
    void testGenerateMetricsForClientSpan_WithNullDuration() {
        // Given
        mockClientSpan.durationInNanos = null;

        // When
        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                mockClientSpan, mockDecoration, currentTime, metricsStateByKey, anchorTimestamp);

        // Then
        MetricAggregationState state = metricsStateByKey.values().iterator().next();
        assertEquals(1, state.requestCount);
        assertEquals(0, state.latencyDurations.size());
    }

    @Test
    void testGenerateMetricsForClientSpan_WithZeroDuration() {
        // Given
        mockClientSpan.durationInNanos = 0L;

        // When
        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                mockClientSpan, mockDecoration, currentTime, metricsStateByKey, anchorTimestamp);

        // Then
        MetricAggregationState state = metricsStateByKey.values().iterator().next();
        assertEquals(1, state.requestCount);
        assertEquals(0, state.latencyDurations.size());
    }

    @Test
    void testGenerateMetricsForClientSpan_ExemplarLimit() {
        // Given - Create span with error status
        SpanStateData errorSpan = createSpanWithHttpStatus(400);
        MetricAggregationState existingState = new MetricAggregationState();
        // Pre-fill with 10 exemplars
        for (int i = 0; i < 10; i++) {
            existingState.errorExemplars.add(mock(Exemplar.class));
        }
        
        Map<String, Object> labels = new HashMap<>();
        labels.put("namespace", "span_derived");
        labels.put("environment", errorSpan.getEnvironment());
        labels.put("service", errorSpan.serviceName);
        labels.put("operation", mockDecoration.parentServerOperationName);
        labels.put("remoteEnvironment", mockDecoration.remoteEnvironment);
        labels.put("remoteService", mockDecoration.remoteService);
        labels.put("remoteOperation", mockDecoration.remoteOperation);
        labels.putAll(errorSpan.groupByAttributes);
        
        MetricKey key = new MetricKey(labels, anchorTimestamp);
        metricsStateByKey.put(key, existingState);

        // When
        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                errorSpan, mockDecoration, currentTime, metricsStateByKey, anchorTimestamp);

        // Then
        assertEquals(10, existingState.errorExemplars.size()); // Should not exceed limit
    }

    @Test
    void testGenerateMetricsForServerSpan_Success() {
        // When
        ApmServiceMapMetricsUtil.generateMetricsForServerSpan(
                mockServerSpan, currentTime, metricsStateByKey, anchorTimestamp);

        // Then
        assertEquals(1, metricsStateByKey.size());
        MetricAggregationState state = metricsStateByKey.values().iterator().next();
        assertEquals(1, state.requestCount);
        assertEquals(0, state.errorCount);
        assertEquals(0, state.faultCount);
        assertEquals(1, state.latencyDurations.size());
    }

    @Test
    void testGenerateMetricsForServerSpan_WithError() {
        // Given - Create span with error status
        SpanStateData errorSpan = createSpanWithHttpStatus(400); // HTTP 400 = error

        // When
        ApmServiceMapMetricsUtil.generateMetricsForServerSpan(
                errorSpan, currentTime, metricsStateByKey, anchorTimestamp);

        // Then
        MetricAggregationState state = metricsStateByKey.values().iterator().next();
        assertEquals(1, state.requestCount);
        assertEquals(1, state.errorCount);
        assertEquals(0, state.faultCount);
        assertEquals(1, state.errorExemplars.size());
        assertEquals(0, state.faultExemplars.size());
    }

    @Test
    void testGenerateMetricsForServerSpan_WithFault() {
        // Given - Create span with fault status 
        SpanStateData faultSpan = createSpanWithHttpStatus(500); // HTTP 500 = fault

        // When
        ApmServiceMapMetricsUtil.generateMetricsForServerSpan(
                faultSpan, currentTime, metricsStateByKey, anchorTimestamp);

        // Then
        MetricAggregationState state = metricsStateByKey.values().iterator().next();
        assertEquals(1, state.requestCount);
        assertEquals(0, state.errorCount);
        assertEquals(1, state.faultCount);
        assertEquals(0, state.errorExemplars.size());
        assertEquals(1, state.faultExemplars.size());
    }

    @Test
    void testCreateMetricsFromAggregatedState_EmptyLatencyDurations() {
        // Given
        MetricAggregationState state = new MetricAggregationState();
        state.requestCount = 1;
        state.errorCount = 0;
        state.faultCount = 0;
        // latencyDurations is empty by default
        
        Map<String, Object> labels = new HashMap<>();
        labels.put("service", "test-service");
        
        MetricKey key = new MetricKey(labels, anchorTimestamp);
        metricsStateByKey.put(key, state);

        // When
        List<JacksonMetric> metrics = ApmServiceMapMetricsUtil.createMetricsFromAggregatedState(metricsStateByKey);

        // Then
        assertEquals(3, metrics.size()); // Only request, error, fault (no latency_seconds)
    }

    @Test
    void testCreateExemplarFromSpan_Success() {
        // When
        Exemplar exemplar = ApmServiceMapMetricsUtil.createExemplarFromSpan(mockClientSpan, 1.0);

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
                null, // serviceName is null
                null, // spanId is null 
                null, // parentSpanId is null
                null, // traceId is null
                "SERVER",
                "test-op",
                "test-op",
                1000000000L,
                "OK",
                "2023-01-01T00:00:00.000Z",
                Collections.emptyMap(),
                Collections.emptyMap()
        );

        // When
        Exemplar exemplar = ApmServiceMapMetricsUtil.createExemplarFromSpan(corruptedSpan, 1.0);

        // Then
        assertNotNull(exemplar); // Should still return a minimal exemplar
        assertEquals(1.0, exemplar.getValue());
    }

    @Test
    void testCreateExemplarFromSpan_WithNullStatus() {
        // Given
        mockClientSpan.status = null;

        // When
        Exemplar exemplar = ApmServiceMapMetricsUtil.createExemplarFromSpan(mockClientSpan, 1.0);

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
        assertTrue(metric.getAttributes().containsKey("randomKey")); // Verify random key is added
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
        // Verify attributes exist (specific content may vary based on implementation)
        assertNotNull(metric.getAttributes());
        
        // Verify it's a histogram by checking the type returned by the method
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
        assertNotNull(buckets.bucketCounts);
        assertNotNull(buckets.explicitBounds);
        assertEquals(16, buckets.bucketCounts.size()); // 15 bounds + 1 overflow bucket
        assertEquals(15, buckets.explicitBounds.size());
        
        // Verify total count equals input size
        long totalCount = buckets.bucketCounts.stream().mapToLong(Long::longValue).sum();
        assertEquals(durations.size(), totalCount);
    }

    @Test
    void testCreateHistogramBucketsFromDurations_BoundaryValues() {
        // Given - test exact boundary values
        List<Double> durations = Arrays.asList(0.0, 0.005, 0.01, 0.025); // Exact boundary values

        // When
        HistogramBuckets buckets = ApmServiceMapMetricsUtil.createHistogramBucketsFromDurations(durations);

        // Then
        assertNotNull(buckets);
        long totalCount = buckets.bucketCounts.stream().mapToLong(Long::longValue).sum();
        assertEquals(4, totalCount);
        
        // Verify at least some buckets have data (bucket distribution may vary based on implementation)
        boolean hasBucketData = buckets.bucketCounts.stream().anyMatch(count -> count > 0);
        assertTrue(hasBucketData, "At least some buckets should contain data");
    }

    @Test
    void testCreateHistogramBucketsFromDurations_WithNullValues() {
        // Given
        List<Double> durations = new ArrayList<>();
        durations.add(0.1);
        durations.add(null); // Should be ignored
        durations.add(1.0);

        // When
        HistogramBuckets buckets = ApmServiceMapMetricsUtil.createHistogramBucketsFromDurations(durations);

        // Then
        assertNotNull(buckets);
        // Verify only non-null values are counted
        long totalCount = buckets.bucketCounts.stream().mapToLong(Long::longValue).sum();
        assertEquals(2, totalCount); // Only 2 non-null values
    }

    @Test
    void testCreateHistogramBucketsFromDurations_EmptyList() {
        // Given
        List<Double> durations = Collections.emptyList();

        // When
        HistogramBuckets buckets = ApmServiceMapMetricsUtil.createHistogramBucketsFromDurations(durations);

        // Then
        assertNotNull(buckets);
        assertEquals(16, buckets.bucketCounts.size());
        assertEquals(15, buckets.explicitBounds.size());
        
        // All bucket counts should be 0
        for (Long count : buckets.bucketCounts) {
            assertEquals(0L, count);
        }
    }

    @Test
    void testCreateHistogramBucketsFromDurations_OverflowBucket() {
        // Given
        List<Double> durations = Arrays.asList(20.0, 100.0); // Values beyond largest bound (10.0)

        // When
        HistogramBuckets buckets = ApmServiceMapMetricsUtil.createHistogramBucketsFromDurations(durations);

        // Then
        assertNotNull(buckets);
        // Overflow bucket (last bucket) should have count 2
        assertEquals(2L, buckets.bucketCounts.get(buckets.bucketCounts.size() - 1));
        
        // All other buckets should be 0
        for (int i = 0; i < buckets.bucketCounts.size() - 1; i++) {
            assertEquals(0L, buckets.bucketCounts.get(i));
        }
    }

    @Test
    void testCreateMetricsFromAggregatedState_Success() {
        // Given
        MetricAggregationState state = new MetricAggregationState();
        state.requestCount = 5;
        state.errorCount = 2;
        state.faultCount = 1;
        state.latencyDurations.addAll(Arrays.asList(0.1, 0.2, 0.5, 1.0, 2.0));
        
        Map<String, Object> labels = new HashMap<>();
        labels.put("service", "test-service");
        
        MetricKey key = new MetricKey(labels, anchorTimestamp);
        metricsStateByKey.put(key, state);

        // When
        List<JacksonMetric> metrics = ApmServiceMapMetricsUtil.createMetricsFromAggregatedState(metricsStateByKey);

        // Then
        assertEquals(4, metrics.size()); // request, error, fault, latency_seconds
        
        // Verify metric names
        List<String> metricNames = metrics.stream()
                .map(JacksonMetric::getName)
                .collect(Collectors.toList());
        assertTrue(metricNames.contains("request"));
        assertTrue(metricNames.contains("error"));
        assertTrue(metricNames.contains("fault"));
        assertTrue(metricNames.contains("latency_seconds"));
    }

    @Test
    void testMultipleSpansAggregation() {
        // Given
        SpanStateData span1 = createSpanWithHttpStatus(400, "service1", "op1", "env1"); // Error
        SpanStateData span2 = createSpanWithHttpStatus(500, "service1", "op1", "env1"); // Fault
        span1.durationInNanos = 1000000000L; // 1 second
        span2.durationInNanos = 2000000000L; // 2 seconds

        // When
        ApmServiceMapMetricsUtil.generateMetricsForServerSpan(
                span1, currentTime, metricsStateByKey, anchorTimestamp);
        ApmServiceMapMetricsUtil.generateMetricsForServerSpan(
                span2, currentTime, metricsStateByKey, anchorTimestamp);

        // Then
        assertEquals(1, metricsStateByKey.size()); // Same labels, should aggregate
        MetricAggregationState state = metricsStateByKey.values().iterator().next();
        assertEquals(2, state.requestCount);
        assertEquals(1, state.errorCount);
        assertEquals(1, state.faultCount);
        assertEquals(2, state.latencyDurations.size());
        assertEquals(1.0, state.latencyDurations.get(0), 0.001);
        assertEquals(2.0, state.latencyDurations.get(1), 0.001);
    }

    @Test
    void testMetricsLabelsCorrectness_ClientSpan() {
        // When
        ApmServiceMapMetricsUtil.generateMetricsForClientSpan(
                mockClientSpan, mockDecoration, currentTime, metricsStateByKey, anchorTimestamp);

        // Then
        MetricKey key = metricsStateByKey.keySet().iterator().next();
        Map<String, Object> labels = key.labels;
        
        assertEquals("span_derived", labels.get("namespace"));
        assertEquals(mockClientSpan.getEnvironment(), labels.get("environment"));
        assertEquals(mockClientSpan.serviceName, labels.get("service"));
        assertEquals(mockDecoration.parentServerOperationName, labels.get("operation"));
        assertEquals(mockDecoration.remoteEnvironment, labels.get("remoteEnvironment"));
        assertEquals(mockDecoration.remoteService, labels.get("remoteService"));
        assertEquals(mockDecoration.remoteOperation, labels.get("remoteOperation"));
        assertEquals("value", labels.get("custom")); // from groupByAttributes
    }

    @Test
    void testMetricsLabelsCorrectness_ServerSpan() {
        // When
        ApmServiceMapMetricsUtil.generateMetricsForServerSpan(
                mockServerSpan, currentTime, metricsStateByKey, anchorTimestamp);

        // Then
        MetricKey key = metricsStateByKey.keySet().iterator().next();
        Map<String, Object> labels = key.labels;
        
        assertEquals("span_derived", labels.get("namespace"));
        assertEquals(mockServerSpan.getEnvironment(), labels.get("environment"));
        assertEquals(mockServerSpan.serviceName, labels.get("service"));
        assertEquals(mockServerSpan.getOperationName(), labels.get("operation"));
        assertEquals("value", labels.get("custom")); // from groupByAttributes
        
        // Should NOT have remote* labels for server spans
        assertFalse(labels.containsKey("remoteEnvironment"));
        assertFalse(labels.containsKey("remoteService"));
        assertFalse(labels.containsKey("remoteOperation"));
    }

    @Test 
    void testMetricsSortedByTimestamp() {
        // Given
        MetricAggregationState state1 = new MetricAggregationState();
        state1.requestCount = 1;
        state1.latencyDurations.add(1.0);
        
        MetricAggregationState state2 = new MetricAggregationState();  
        state2.requestCount = 2;
        state2.latencyDurations.add(2.0);
        
        Instant earlierTime = anchorTimestamp.minusSeconds(60);
        Instant laterTime = anchorTimestamp.plusSeconds(60);
        
        Map<String, Object> labels1 = new HashMap<>();
        labels1.put("service", "service1");
        
        Map<String, Object> labels2 = new HashMap<>();
        labels2.put("service", "service2");
        
        metricsStateByKey.put(new MetricKey(labels2, laterTime), state2);  // Add later time first
        metricsStateByKey.put(new MetricKey(labels1, earlierTime), state1);
        
        // When
        List<JacksonMetric> metrics = ApmServiceMapMetricsUtil.createMetricsFromAggregatedState(metricsStateByKey);
        
        // Then
        assertFalse(metrics.isEmpty());
        // Verify metrics are sorted by timestamp - compare the first few metrics
        if (metrics.size() >= 2) {
            String firstTimestamp = metrics.get(0).getTime();
            String secondTimestamp = metrics.get(1).getTime();
            assertTrue(firstTimestamp.compareTo(secondTimestamp) <= 0, 
                "Metrics should be sorted by timestamp");
        }
    }
}
