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

import org.apache.commons.lang3.RandomStringUtils;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import java.util.stream.Stream;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.Gauge;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.Sum;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.Histogram;
import org.opensearch.dataprepper.model.metric.DefaultBucket;
import org.opensearch.dataprepper.model.metric.Bucket;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PrometheusTimeSeriesTest {
    @ParameterizedTest
    @MethodSource("getLabelNames")
    public void testSanitizationOfLabelNames(final String labelName, final String expectedMetricName) {
        String sanitizedName = PrometheusTimeSeries.sanitizeLabelName(labelName);
        assertThat(sanitizedName, equalTo(expectedMetricName));
    }

    @ParameterizedTest
    @MethodSource("getMetricNames")
    public void testSanitizationOfMetricNames(final String metricName, final String expectedMetricName) {
        Histogram histogram = createHistogramMetric(metricName, "1");
        String sanitizedName = PrometheusTimeSeries.sanitizeMetricName(histogram);
        assertThat(sanitizedName, equalTo(expectedMetricName));
    }

    @ParameterizedTest
    @MethodSource("getGaugeMetricDifferentUnitTypes")
    public void testGaugeMetricNameSanitizationDifferentUnitTypes(final String unit, final String expectedUnit) {
        final String name = RandomStringUtils.randomAlphabetic(10);
        Gauge gauge = createGaugeMetric(name, unit);
        String sanitizedName = PrometheusTimeSeries.sanitizeMetricName(gauge);
        if (expectedUnit.equals("")) {
            assertThat(sanitizedName, equalTo(name));
        } else {
            assertThat(sanitizedName, equalTo(name+"_"+expectedUnit));
        }
    }

    @ParameterizedTest
    @MethodSource("getCommonMetricDifferentUnitTypes")
    public void testCounterTypeSumMetricNameSanitizationDifferentUnitTypes(final String unit, final String expectedUnit) {
        final String name = RandomStringUtils.randomAlphabetic(10);
        Sum sum = createSumMetric(name, unit, true, "AGGREGATION_TEMPORALITY_CUMULATIVE");
        String sanitizedName = PrometheusTimeSeries.sanitizeMetricName(sum);
        if (expectedUnit.equals("")) {
            assertThat(sanitizedName, equalTo(name+"_total"));
        } else {
            assertThat(sanitizedName, equalTo(name+"_"+expectedUnit+"_total"));
        }
    }

    @ParameterizedTest
    @MethodSource("getCommonMetricDifferentUnitTypes")
    public void testGaugeTypeSumMetricNameSanitizationDifferentUnitTypes(final String unit, final String expectedUnit) {
        final String name = RandomStringUtils.randomAlphabetic(10);
        List<Sum> sumMetrics = new ArrayList<>();
        sumMetrics.add(createSumMetric(name, unit, false, "AGGREGATION_TEMPORALITY_DELTA"));
        sumMetrics.add(createSumMetric(name, unit, false, "AGGREGATION_TEMPORALITY_CUMULATIVE"));
        sumMetrics.add(createSumMetric(name, unit, true, "AGGREGATION_TEMPORALITY_DELTA"));
        for (final Sum sum: sumMetrics) {
            String sanitizedName = PrometheusTimeSeries.sanitizeMetricName(sum);
            if (expectedUnit.equals("")) {
                assertThat(sanitizedName, equalTo(name));
            } else {
                assertThat(sanitizedName, equalTo(name+"_"+expectedUnit));
            }
        }
    }

    @ParameterizedTest
    @MethodSource("getCommonMetricDifferentUnitTypes")
    public void testHistogramMetricNameSanitizationDifferentUnitTypes(final String unit, final String expectedUnit) {
        final String name = RandomStringUtils.randomAlphabetic(10);
        Histogram histogram = createHistogramMetric(name, unit);
        String sanitizedName = PrometheusTimeSeries.sanitizeMetricName(histogram);
        if (expectedUnit.equals("")) {
            assertThat(sanitizedName, equalTo(name));
        } else {
            assertThat(sanitizedName, equalTo(name+"_"+expectedUnit));
        }
    }

    private Histogram createHistogramMetric(final String name, final String unit) {
        final List<Bucket> TEST_BUCKETS = Arrays.asList(
                new DefaultBucket(Double.NEGATIVE_INFINITY, 5.0, 2222L),
                new DefaultBucket(5.0, 10.0, 5555L),
                new DefaultBucket(10.0, 100.0, 3333L),
                new DefaultBucket(100.0, Double.POSITIVE_INFINITY, 7777L)
        );
        final List<Long> TEST_BUCKET_COUNTS_LIST = Arrays.asList(2222L, 5555L, 3333L, 7777L);
        final List<Double> TEST_EXPLICIT_BOUNDS_LIST = Arrays.asList(5D, 10D, 100D);
        return JacksonHistogram.builder()
            .withName(name)
            .withDescription("Test Histogram Metric")
            .withTimeReceived(Instant.now())
            .withTime(Instant.now().plusSeconds(10).toString())
            .withStartTime(Instant.now().plusSeconds(5).toString())
            .withUnit(unit)
            .withSum(1)
            .withMin(2.0d)
            .withMax(3.0d)
            .withCount(10L)
            .withBucketCount(TEST_BUCKETS.size())
            .withExplicitBoundsCount(TEST_EXPLICIT_BOUNDS_LIST.size())
            .withAggregationTemporality("cumulative")
            .withBuckets(TEST_BUCKETS)
            .withBucketCountsList(TEST_BUCKET_COUNTS_LIST)
            .withExplicitBoundsList(TEST_EXPLICIT_BOUNDS_LIST)
            .build(false);
    }

    private Sum createSumMetric(final String name, final String unit, final boolean isMonotonic, final String aggregationTemporality) {
        return JacksonSum.builder()
            .withName(name)
            .withDescription("Test Sum Metric")
            .withTimeReceived(Instant.now())
            .withTime(Instant.now().plusSeconds(10).toString())
            .withStartTime(Instant.now().plusSeconds(5).toString())
            .withIsMonotonic(isMonotonic)
            .withUnit(unit)
            .withAggregationTemporality(aggregationTemporality)
            .withValue(1.0d)
            .build(false);
    }

    private Gauge createGaugeMetric(final String name, final String unit) {
        return JacksonGauge.builder()
            .withName(name)
            .withDescription("Test Gauge Metric")
            .withTimeReceived(Instant.now())
            .withTime(Instant.now().plusSeconds(10).toString())
            .withStartTime(Instant.now().plusSeconds(5).toString())
            .withUnit(unit)
            .withValue(1.0d)
            .build(false);
    }

    private static Stream<Arguments> getLabelNames() {
        return Stream.of(
            arguments("label1", "label1"),
            arguments("label:1", "label_1"),
            arguments("label.1", "label_1"),
            arguments("_label1", "_label1"),
            arguments("label1_", "label1_"),
            arguments("___label1__", "___label1__"),
            arguments("name (of the host)", "name__of_the_host_")
        );
    }

    private static Stream<Arguments> getMetricNames() {
        return Stream.of(
            arguments("metric1", "metric1"),
            arguments("metric:1", "metric:1"),
            arguments("metric_1", "metric_1"),
            arguments("((metric1))", "metric1"),
            arguments("&*#^%&#*$%metric1))", "metric1"),
            arguments("metric1&*#^%&#*$%))", "metric1"),
            arguments("metric&*#^%&#*$%1))", "metric_1"),
            arguments("_metric$1", "metric_1"),
            arguments("_metric^$1", "metric_1"),
            arguments("(metric1)", "metric1")
        );
    }

    private static Stream<Arguments> getGaugeMetricDifferentUnitTypes() {
        return Stream.concat(getMetricDifferentUnitTypes(), Stream.of(arguments("1", "ratio")));
    }

    private static Stream<Arguments> getCommonMetricDifferentUnitTypes() {
        return Stream.concat(getMetricDifferentUnitTypes(), Stream.of(arguments("1", "")));
    }

    private static Stream<Arguments> getMetricDifferentUnitTypes() {
        return Stream.of(
            arguments("d",    "days"),
            arguments("h",    "hours"),
            arguments("min",  "minutes"),
            arguments("s",    "seconds"),
            arguments("ms",   "milliseconds"),
            arguments("us",   "microseconds"),
            arguments("ns",   "nanoseconds"),
            arguments("By",   "bytes"),
            arguments("KiBy", "kibibytes"),
            arguments("MiBy", "mebibytes"),
            arguments("GiBy", "gibibytes"),
            arguments("TiBy", "tibibytes"),
            arguments("KBy",  "kilobytes"),
            arguments("MBy",  "megabytes"),
            arguments("GBy",  "gigabytes"),
            arguments("TBy",  "terabytes"),
            arguments("V",    "volts"),
            arguments("A",    "amperes"),
            arguments("J",    "joules"),
            arguments("W",    "watts"),
            arguments("g",    "grams"),
            arguments("Cel",  "celsius"),
            arguments("Hz",   "hertz"),
            arguments("%",    "percent"),
            arguments("m",    "meters"),
            arguments("{packets}",    ""),
            arguments("packets",    "packets")
        );
    }
}
