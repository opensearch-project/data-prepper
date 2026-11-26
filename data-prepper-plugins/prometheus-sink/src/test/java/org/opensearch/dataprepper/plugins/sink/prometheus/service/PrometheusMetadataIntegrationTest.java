/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.prometheus.service;

import com.arpnetworking.metrics.prometheus.Remote;
import com.arpnetworking.metrics.prometheus.Types;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.metric.DefaultBucket;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * Integration test that verifies the complete flow of metadata generation,
 * serialization, and injection into WriteRequest protobuf.
 */
public class PrometheusMetadataIntegrationTest {

    @Test
    public void testEndToEndMetadataFlow() throws Exception {
        // Create test metrics
        JacksonGauge gauge = createGaugeMetric("test_gauge", "ms", "Test gauge metric");
        JacksonSum counter = createCounterMetric("test_counter", "By", "Test counter metric");
        JacksonHistogram histogram = createHistogramMetric("test_histogram", "s", "Test histogram metric");

        // Convert to PrometheusTimeSeries with metadata
        PrometheusTimeSeries gaugeSeries = new PrometheusTimeSeries(gauge, true);
        PrometheusTimeSeries counterSeries = new PrometheusTimeSeries(counter, true);
        PrometheusTimeSeries histogramSeries = new PrometheusTimeSeries(histogram, true);

        // Collect all time series and metadata
        List<Types.TimeSeries> allTimeSeries = new ArrayList<>();
        allTimeSeries.addAll(gaugeSeries.getTimeSeriesList());
        allTimeSeries.addAll(counterSeries.getTimeSeriesList());
        allTimeSeries.addAll(histogramSeries.getTimeSeriesList());

        List<PrometheusMetricMetadata> metadataList = Arrays.asList(
                gaugeSeries.getMetadata(),
                counterSeries.getMetadata(),
                histogramSeries.getMetadata()
        );

        // Create WriteRequest with time series
        Remote.WriteRequest.Builder builder = Remote.WriteRequest.newBuilder();
        builder.addAllTimeseries(allTimeSeries);
        Remote.WriteRequest request = builder.build();
        byte[] originalBytes = request.toByteArray();

        // Inject metadata
        byte[] withMetadata = PrometheusMetadataSerializer.injectMetadata(originalBytes, metadataList);

        // Verify size increased
        assertThat(withMetadata.length, greaterThan(originalBytes.length));

        // Verify original data is preserved (first N bytes should match)
        for (int i = 0; i < originalBytes.length; i++) {
            assertThat("Original byte at position " + i + " should be preserved",
                    withMetadata[i], equalTo(originalBytes[i]));
        }

        // Verify metadata was added (additional bytes exist)
        int metadataSize = withMetadata.length - originalBytes.length;
        assertThat(metadataSize, greaterThan(0));

        // Estimate should be close to actual
        int estimatedSize = PrometheusMetadataSerializer.estimateMetadataSize(metadataList);
        // Allow 20% margin for protobuf overhead
        assertThat(metadataSize, lessThan((int) (estimatedSize * 1.2)));
        assertThat(metadataSize, greaterThan((int) (estimatedSize * 0.8)));
    }

    @Test
    public void testMetadataDeduplicationInFlow() throws Exception {
        // Create multiple metrics with the same family name
        JacksonGauge gauge1 = createGaugeMetric("test_metric", "1", "Description 1");
        JacksonGauge gauge2 = createGaugeMetric("test_metric", "1", "Description 2");
        JacksonGauge gauge3 = createGaugeMetric("test_metric", "1", "Description 3");

        PrometheusTimeSeries series1 = new PrometheusTimeSeries(gauge1, true);
        PrometheusTimeSeries series2 = new PrometheusTimeSeries(gauge2, true);
        PrometheusTimeSeries series3 = new PrometheusTimeSeries(gauge3, true);

        List<Types.TimeSeries> allTimeSeries = new ArrayList<>();
        allTimeSeries.addAll(series1.getTimeSeriesList());
        allTimeSeries.addAll(series2.getTimeSeriesList());
        allTimeSeries.addAll(series3.getTimeSeriesList());

        List<PrometheusMetricMetadata> metadataList = Arrays.asList(
                series1.getMetadata(),
                series2.getMetadata(),
                series3.getMetadata()
        );

        Remote.WriteRequest request = Remote.WriteRequest.newBuilder()
                .addAllTimeseries(allTimeSeries)
                .build();
        byte[] originalBytes = request.toByteArray();

        byte[] withMetadata = PrometheusMetadataSerializer.injectMetadata(originalBytes, metadataList);

        // Should only add one metadata entry due to deduplication
        int singleMetadataSize = PrometheusMetadataSerializer.estimateMetadataSize(
                Collections.singletonList(series1.getMetadata()));
        int actualAdded = withMetadata.length - originalBytes.length;

        // Actual size should be close to single metadata size (not triple)
        assertThat(actualAdded, lessThan(singleMetadataSize * 2));
    }

    @Test
    public void testLargeMetricBatchWithMetadata() throws Exception {
        // Simulate a large batch of metrics
        List<Types.TimeSeries> allTimeSeries = new ArrayList<>();
        List<PrometheusMetricMetadata> metadataList = new ArrayList<>();

        // Create 100 different metrics
        for (int i = 0; i < 100; i++) {
            JacksonGauge gauge = createGaugeMetric("metric_" + i, "1", "Metric number " + i);
            PrometheusTimeSeries series = new PrometheusTimeSeries(gauge, true);
            allTimeSeries.addAll(series.getTimeSeriesList());
            metadataList.add(series.getMetadata());
        }

        Remote.WriteRequest request = Remote.WriteRequest.newBuilder()
                .addAllTimeseries(allTimeSeries)
                .build();
        byte[] originalBytes = request.toByteArray();

        byte[] withMetadata = PrometheusMetadataSerializer.injectMetadata(originalBytes, metadataList);

        // Verify metadata overhead is reasonable
        // For 100 metrics with minimal samples, overhead can be higher percentage-wise
        // but absolute size should be reasonable
        int overhead = withMetadata.length - originalBytes.length;
        double overheadPercent = (overhead * 100.0) / originalBytes.length;

        // Metadata overhead should be less than 100% (i.e., less than doubling the size)
        assertThat(overheadPercent, lessThan(100.0));

        // Verify estimated size is accurate
        int estimatedSize = PrometheusMetadataSerializer.estimateMetadataSize(metadataList);
        assertThat(overhead, lessThan((int) (estimatedSize * 1.3)));
        assertThat(overhead, greaterThan((int) (estimatedSize * 0.7)));
    }

    @Test
    public void testMetadataWithAllFieldTypes() throws Exception {
        // Test with full metadata (all fields populated)
        JacksonSum counter = JacksonSum.builder()
                .withName("full_metadata_counter")
                .withDescription("This is a very detailed description of the counter metric that " +
                        "explains what it measures and why it's important for monitoring")
                .withUnit("bytes")
                .withTimeReceived(Instant.now())
                .withTime(Instant.now().toString())
                .withValue(100.0)
                .withIsMonotonic(true)
                .withAggregationTemporality("AGGREGATION_TEMPORALITY_CUMULATIVE")
                .withAttributes(Map.of("env", "prod", "service", "api"))
                .build(false);

        PrometheusTimeSeries series = new PrometheusTimeSeries(counter, true);
        PrometheusMetricMetadata metadata = series.getMetadata();

        // Verify all fields are populated
        assertThat(metadata.getType(), equalTo(PrometheusMetricMetadata.MetricType.COUNTER));
        assertThat(metadata.getMetricFamilyName(), not(emptyString()));
        assertThat(metadata.getHelp(), not(emptyString()));
        assertThat(metadata.getUnit(), equalTo("bytes"));

        // Inject into WriteRequest
        Remote.WriteRequest request = Remote.WriteRequest.newBuilder()
                .addAllTimeseries(series.getTimeSeriesList())
                .build();
        byte[] originalBytes = request.toByteArray();

        byte[] withMetadata = PrometheusMetadataSerializer.injectMetadata(originalBytes,
                List.of(metadata));

        assertThat(withMetadata.length, greaterThan(originalBytes.length));
    }

    @Test
    public void testMetadataWithEmptyFields() throws Exception {
        // Test with minimal metadata (empty help/unit)
        JacksonGauge gauge = JacksonGauge.builder()
                .withName("minimal_gauge")
                .withDescription("")  // Empty description
                .withUnit("")  // Empty unit
                .withTimeReceived(Instant.now())
                .withTime(Instant.now().toString())
                .withValue(42.0)
                .withAttributes(Map.of())
                .build(false);

        PrometheusTimeSeries series = new PrometheusTimeSeries(gauge, true);
        PrometheusMetricMetadata metadata = series.getMetadata();

        Remote.WriteRequest request = Remote.WriteRequest.newBuilder()
                .addAllTimeseries(series.getTimeSeriesList())
                .build();
        byte[] originalBytes = request.toByteArray();

        byte[] withMetadata = PrometheusMetadataSerializer.injectMetadata(originalBytes,
                Collections.singletonList(metadata));

        // Should still work with empty fields
        assertThat(withMetadata.length, greaterThan(originalBytes.length));

        // Overhead should be minimal
        int overhead = withMetadata.length - originalBytes.length;
        assertThat(overhead, lessThan(50)); // Small overhead for minimal metadata
    }

    @Test
    public void testMetadataPreservesTimeSeriesData() throws Exception {
        // Create a metric with specific sample values
        double expectedValue = 123.456;
        long expectedTimestamp = System.currentTimeMillis();

        JacksonGauge gauge = createGaugeMetric("precise_gauge", "s", "Precise gauge");
        PrometheusTimeSeries series = new PrometheusTimeSeries(gauge, true);

        // Build WriteRequest
        Remote.WriteRequest request = Remote.WriteRequest.newBuilder()
                .addAllTimeseries(series.getTimeSeriesList())
                .build();
        byte[] originalBytes = request.toByteArray();

        // Inject metadata
        byte[] withMetadata = PrometheusMetadataSerializer.injectMetadata(originalBytes,
                Collections.singletonList(series.getMetadata()));

        // Parse original request to verify data
        Remote.WriteRequest originalRequest = Remote.WriteRequest.parseFrom(originalBytes);
        assertThat(originalRequest.getTimeseriesCount(), equalTo(series.getTimeSeriesList().size()));

        // Verify that reading the first N bytes of withMetadata gives us the original request
        byte[] extractedOriginal = Arrays.copyOf(withMetadata, originalBytes.length);
        Remote.WriteRequest extractedRequest = Remote.WriteRequest.parseFrom(extractedOriginal);
        assertThat(extractedRequest.getTimeseriesCount(), equalTo(originalRequest.getTimeseriesCount()));
    }

    // Helper methods

    private JacksonGauge createGaugeMetric(String name, String unit, String description) {
        return JacksonGauge.builder()
                .withName(name)
                .withDescription(description)
                .withTimeReceived(Instant.now())
                .withTime(Instant.now().toString())
                .withUnit(unit)
                .withValue(42.0)
                .withAttributes(Map.of("env", "test"))
                .build(false);
    }

    private JacksonSum createCounterMetric(String name, String unit, String description) {
        return JacksonSum.builder()
                .withName(name)
                .withDescription(description)
                .withTimeReceived(Instant.now())
                .withTime(Instant.now().toString())
                .withUnit(unit)
                .withValue(100.0)
                .withIsMonotonic(true)
                .withAggregationTemporality("AGGREGATION_TEMPORALITY_CUMULATIVE")
                .withAttributes(Map.of("env", "test"))
                .build(false);
    }

    private JacksonHistogram createHistogramMetric(String name, String unit, String description) {
        return JacksonHistogram.builder()
                .withName(name)
                .withDescription(description)
                .withTimeReceived(Instant.now())
                .withTime(Instant.now().toString())
                .withUnit(unit)
                .withSum(150.0)
                .withCount(10L)
                .withBuckets(Arrays.asList(
                        new DefaultBucket(0.0, 1.0, 2L),
                        new DefaultBucket(1.0, 5.0, 5L),
                        new DefaultBucket(5.0, Double.POSITIVE_INFINITY, 3L)
                ))
                .withAttributes(Map.of("env", "test"))
                .build(false);
    }
}
