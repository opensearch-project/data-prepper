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

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.metric.DefaultBucket;
import org.opensearch.dataprepper.model.metric.DefaultQuantile;
import org.opensearch.dataprepper.model.metric.ExponentialHistogram;
import org.opensearch.dataprepper.model.metric.Gauge;
import org.opensearch.dataprepper.model.metric.Histogram;
import org.opensearch.dataprepper.model.metric.JacksonExponentialHistogram;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonSummary;
import org.opensearch.dataprepper.model.metric.Sum;
import org.opensearch.dataprepper.model.metric.Summary;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

public class PrometheusMetricMetadataTest {

    @Test
    public void testGaugeMetricMetadata() {
        Gauge gauge = createGaugeMetric("test_gauge", "ms", "Test gauge description");
        PrometheusMetricMetadata metadata = PrometheusMetricMetadata.fromMetric(gauge, "test_gauge_milliseconds");

        assertThat(metadata.getType(), equalTo(PrometheusMetricMetadata.MetricType.GAUGE));
        assertThat(metadata.getMetricFamilyName(), equalTo("test_gauge_milliseconds"));
        assertThat(metadata.getHelp(), equalTo("Test gauge description"));
        assertThat(metadata.getUnit(), equalTo("ms"));
    }

    @Test
    public void testCounterMetricMetadata() {
        Sum counter = createSumMetric("test_counter", "By", "Test counter description",
                true, "AGGREGATION_TEMPORALITY_CUMULATIVE");
        PrometheusMetricMetadata metadata = PrometheusMetricMetadata.fromMetric(counter, "test_counter_bytes_total");

        assertThat(metadata.getType(), equalTo(PrometheusMetricMetadata.MetricType.COUNTER));
        assertThat(metadata.getMetricFamilyName(), equalTo("test_counter_bytes_total"));
        assertThat(metadata.getHelp(), equalTo("Test counter description"));
        assertThat(metadata.getUnit(), equalTo("By"));
    }

    @Test
    public void testNonMonotonicSumMetricMetadata() {
        Sum sum = createSumMetric("test_sum", "1", "Test sum description",
                false, "AGGREGATION_TEMPORALITY_DELTA");
        PrometheusMetricMetadata metadata = PrometheusMetricMetadata.fromMetric(sum, "test_sum");

        // Non-monotonic sums are treated as gauges
        assertThat(metadata.getType(), equalTo(PrometheusMetricMetadata.MetricType.GAUGE));
    }

    @Test
    public void testHistogramMetricMetadata() {
        Histogram histogram = createHistogramMetric("test_histogram", "s", "Test histogram description");
        PrometheusMetricMetadata metadata = PrometheusMetricMetadata.fromMetric(histogram, "test_histogram_seconds");

        assertThat(metadata.getType(), equalTo(PrometheusMetricMetadata.MetricType.HISTOGRAM));
        assertThat(metadata.getMetricFamilyName(), equalTo("test_histogram_seconds"));
        assertThat(metadata.getHelp(), equalTo("Test histogram description"));
    }

    @Test
    public void testSummaryMetricMetadata() {
        Summary summary = createSummaryMetric("test_summary", "By", "Test summary description");
        PrometheusMetricMetadata metadata = PrometheusMetricMetadata.fromMetric(summary, "test_summary_bytes");

        assertThat(metadata.getType(), equalTo(PrometheusMetricMetadata.MetricType.SUMMARY));
        assertThat(metadata.getMetricFamilyName(), equalTo("test_summary_bytes"));
    }

    @Test
    public void testExponentialHistogramMetricMetadata() {
        ExponentialHistogram expHistogram = createExponentialHistogramMetric("test_exp_histogram", "ms", "Test exp histogram");
        PrometheusMetricMetadata metadata = PrometheusMetricMetadata.fromMetric(expHistogram, "test_exp_histogram_milliseconds");

        // Exponential histograms are represented as regular histograms in Prometheus
        assertThat(metadata.getType(), equalTo(PrometheusMetricMetadata.MetricType.HISTOGRAM));
    }

    @Test
    public void testMetadataWithNullDescription() {
        Gauge gauge = createGaugeMetric("test_gauge", "1", null);
        PrometheusMetricMetadata metadata = PrometheusMetricMetadata.fromMetric(gauge, "test_gauge");

        assertThat(metadata.getHelp(), equalTo(""));
    }

    @Test
    public void testMetadataWithEmptyUnit() {
        Gauge gauge = createGaugeMetric("test_gauge", "", "Description");
        PrometheusMetricMetadata metadata = PrometheusMetricMetadata.fromMetric(gauge, "test_gauge");

        assertThat(metadata.getUnit(), equalTo(""));
    }

    @Test
    public void testMetadataEstimateSize() {
        PrometheusMetricMetadata metadata = PrometheusMetricMetadata.builder()
                .type(PrometheusMetricMetadata.MetricType.COUNTER)
                .metricFamilyName("test_metric_total")
                .help("This is a test metric")
                .unit("bytes")
                .build();

        int estimatedSize = metadata.estimateSize();

        // Size should be positive and reasonable
        assertThat(estimatedSize, greaterThan(0));
        // Rough estimate: type(2) + name(~20) + help(~25) + unit(~10) = ~57 bytes
        assertThat(estimatedSize, lessThan(100));
    }

    @Test
    public void testMetadataEstimateSizeWithEmptyFields() {
        PrometheusMetricMetadata metadata = PrometheusMetricMetadata.builder()
                .type(PrometheusMetricMetadata.MetricType.GAUGE)
                .metricFamilyName("")
                .help("")
                .unit("")
                .build();

        int estimatedSize = metadata.estimateSize();

        // Only type field should contribute
        assertThat(estimatedSize, equalTo(2)); // 1 byte tag + 1 byte enum value
    }

    @Test
    public void testMetadataEquality() {
        PrometheusMetricMetadata metadata1 = PrometheusMetricMetadata.builder()
                .type(PrometheusMetricMetadata.MetricType.COUNTER)
                .metricFamilyName("test_metric")
                .help("Help text")
                .unit("s")
                .build();

        PrometheusMetricMetadata metadata2 = PrometheusMetricMetadata.builder()
                .type(PrometheusMetricMetadata.MetricType.COUNTER)
                .metricFamilyName("test_metric")
                .help("Help text")
                .unit("s")
                .build();

        assertThat(metadata1, equalTo(metadata2));
        assertThat(metadata1.hashCode(), equalTo(metadata2.hashCode()));
    }

    @Test
    public void testMetadataInequality() {
        PrometheusMetricMetadata metadata1 = PrometheusMetricMetadata.builder()
                .type(PrometheusMetricMetadata.MetricType.COUNTER)
                .metricFamilyName("test_metric")
                .help("Help text")
                .unit("s")
                .build();

        PrometheusMetricMetadata metadata2 = PrometheusMetricMetadata.builder()
                .type(PrometheusMetricMetadata.MetricType.GAUGE)
                .metricFamilyName("test_metric")
                .help("Help text")
                .unit("s")
                .build();

        assertThat(metadata1, not(equalTo(metadata2)));
    }

    @Test
    public void testMetadataToString() {
        PrometheusMetricMetadata metadata = PrometheusMetricMetadata.builder()
                .type(PrometheusMetricMetadata.MetricType.COUNTER)
                .metricFamilyName("test_metric_total")
                .help("Test help")
                .unit("bytes")
                .build();

        String toString = metadata.toString();
        assertThat(toString, containsString("COUNTER"));
        assertThat(toString, containsString("test_metric_total"));
        assertThat(toString, containsString("Test help"));
        assertThat(toString, containsString("bytes"));
    }

    private Gauge createGaugeMetric(String name, String unit, String description) {
        return JacksonGauge.builder()
                .withName(name)
                .withDescription(description)
                .withTimeReceived(Instant.now())
                .withTime(Instant.now().toString())
                .withUnit(unit)
                .withValue(42.0)
                .withAttributes(Map.of())
                .build(false);
    }

    private Sum createSumMetric(String name, String unit, String description,
                                boolean isMonotonic, String aggregationTemporality) {
        return JacksonSum.builder()
                .withName(name)
                .withDescription(description)
                .withTimeReceived(Instant.now())
                .withTime(Instant.now().toString())
                .withUnit(unit)
                .withIsMonotonic(isMonotonic)
                .withAggregationTemporality(aggregationTemporality)
                .withValue(100.0)
                .withAttributes(Map.of())
                .build(false);
    }

    private Histogram createHistogramMetric(String name, String unit, String description) {
        return JacksonHistogram.builder()
                .withName(name)
                .withDescription(description)
                .withTimeReceived(Instant.now())
                .withTime(Instant.now().toString())
                .withUnit(unit)
                .withSum(150.0)
                .withCount(10L)
                .withBuckets(List.of(new DefaultBucket(0.0, 10.0, 5L)))
                .withAttributes(Map.of())
                .build(false);
    }

    private Summary createSummaryMetric(String name, String unit, String description) {
        return JacksonSummary.builder()
                .withName(name)
                .withDescription(description)
                .withTimeReceived(Instant.now())
                .withTime(Instant.now().toString())
                .withUnit(unit)
                .withSum(100.0)
                .withCount(10L)
                .withQuantiles(List.of(new DefaultQuantile(0.5, 50.0)))
                .withAttributes(Map.of())
                .build(false);
    }

    private ExponentialHistogram createExponentialHistogramMetric(String name, String unit, String description) {
        return JacksonExponentialHistogram.builder()
                .withName(name)
                .withDescription(description)
                .withTimeReceived(Instant.now())
                .withTime(Instant.now().toString())
                .withUnit(unit)
                .withSum(200.0)
                .withCount(20)
                .withScale(1)
                .withPositiveOffset(0)
                .withNegativeOffset(0)
                .withPositive(Arrays.asList(1L, 2L, 3L))
                .withAttributes(Map.of())
                .build(false);
    }
}
