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

import com.arpnetworking.metrics.prometheus.Types.Label;
import com.arpnetworking.metrics.prometheus.Types.TimeSeries;

import org.apache.commons.lang3.RandomStringUtils;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.Gauge;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.Sum;
import org.opensearch.dataprepper.model.metric.Summary;
import org.opensearch.dataprepper.model.metric.Quantile;
import org.opensearch.dataprepper.model.metric.DefaultQuantile;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonExponentialHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSummary;
import org.opensearch.dataprepper.model.metric.Histogram;
import org.opensearch.dataprepper.model.metric.ExponentialHistogram;
import org.opensearch.dataprepper.model.metric.DefaultBucket;
import org.opensearch.dataprepper.model.metric.Bucket;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class PrometheusTimeSeriesTest {
    private static final Map<String, Object> TEST_ATTR_MAP = Map.of("attrKey1", 1, "attrKey2", Map.of("attrKey3", "attrValue3"));
    private static final Map<String, Object> TEST_SCOPE_MAP = Map.of("attributes", Map.of("scopeAttrKey", "scopeAttrValue"));
    private static final Map<String, Object> TEST_RESOURCE_MAP = Map.of("attributes", Map.of("rscAttrKey", "rscAttrValue"));
    private static final String TEST_ATTR_MAP_STR1 = "attrKey2_attrKey3 attrValue3 attrKey1 1 ";
    private static final String TEST_ATTR_MAP_STR2 = "attrKey1 1 attrKey2_attrKey3 attrValue3 ";
    private static final String TEST_SCOPE_MAP_STR = "scope_scopeAttrKey scopeAttrValue ";
    private static final String TEST_RESOURCE_MAP_STR = "resource_rscAttrKey rscAttrValue ";
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

    @ParameterizedTest
    @MethodSource("getGaugeMetricDifferentUnitTypes")
    public void testGaugeMetricKey(String unit, String expectedUnitName) throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(10);
        Gauge gauge = createGaugeMetric(name, unit);
        PrometheusTimeSeries timeSeries = new PrometheusTimeSeries(gauge, true, Collections.emptyMap());
        final String sanitizedName = PrometheusTimeSeries.sanitizeMetricName(gauge);

        String expectedKey1 = "__name__ "+sanitizedName+" "+TEST_ATTR_MAP_STR1+TEST_RESOURCE_MAP_STR+TEST_SCOPE_MAP_STR;
        String expectedKey2 = "__name__ "+sanitizedName+" "+TEST_ATTR_MAP_STR2+TEST_RESOURCE_MAP_STR+TEST_SCOPE_MAP_STR;
        assertTrue(timeSeries.getMetricKey().equals(expectedKey1) || timeSeries.getMetricKey().equals(expectedKey2));
    }

    @ParameterizedTest
    @MethodSource("getCommonMetricDifferentUnitTypes")
    public void testSumMetricKey(String unit, String expectedUnitName) throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(10);
        Sum sum = createSumMetric(name, unit, true, "AGGREGATION_TEMPORALITY_CUMULATIVE");
        PrometheusTimeSeries timeSeries = new PrometheusTimeSeries(sum, true, Collections.emptyMap());
        final String sanitizedName = PrometheusTimeSeries.sanitizeMetricName(sum);

        String expectedKey1 = "__name__ "+sanitizedName+" "+TEST_ATTR_MAP_STR1+TEST_RESOURCE_MAP_STR+TEST_SCOPE_MAP_STR;
        String expectedKey2 = "__name__ "+sanitizedName+" "+TEST_ATTR_MAP_STR2+TEST_RESOURCE_MAP_STR+TEST_SCOPE_MAP_STR;
        assertTrue(timeSeries.getMetricKey().equals(expectedKey1) || timeSeries.getMetricKey().equals(expectedKey2));
    }

    @ParameterizedTest
    @MethodSource("getCommonMetricDifferentUnitTypes")
    public void testSumMetricNonMonotonicKey(String unit, String expectedUnitName) throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(10);
        Sum sum = createSumMetric(name, unit, false, "AGGREGATION_TEMPORALITY_DELTA");
        PrometheusTimeSeries timeSeries = new PrometheusTimeSeries(sum, true, Collections.emptyMap());
        final String sanitizedName = PrometheusTimeSeries.sanitizeMetricName(sum);

        String expectedKey1 = "__name__ "+sanitizedName+" "+TEST_ATTR_MAP_STR1+TEST_RESOURCE_MAP_STR+TEST_SCOPE_MAP_STR;
        String expectedKey2 = "__name__ "+sanitizedName+" "+TEST_ATTR_MAP_STR2+TEST_RESOURCE_MAP_STR+TEST_SCOPE_MAP_STR;
        assertTrue(timeSeries.getMetricKey().equals(expectedKey1) || timeSeries.getMetricKey().equals(expectedKey2));
    }

    @ParameterizedTest
    @MethodSource("getCommonMetricDifferentUnitTypes")
    public void testSummaryMetricKey(String unit, String expectedUnitName) throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(10);
        Summary summary = createSummaryMetric(name, unit);
        PrometheusTimeSeries timeSeries = new PrometheusTimeSeries(summary, true, Collections.emptyMap());
        final String sanitizedName = PrometheusTimeSeries.sanitizeMetricName(summary);

        String expectedKey1 = "__name__ "+sanitizedName+"_count "+TEST_ATTR_MAP_STR1+TEST_RESOURCE_MAP_STR+TEST_SCOPE_MAP_STR;
        String expectedKey2 = "__name__ "+sanitizedName+"_count "+TEST_ATTR_MAP_STR2+TEST_RESOURCE_MAP_STR+TEST_SCOPE_MAP_STR;
        assertTrue(timeSeries.getMetricKey().equals(expectedKey1) || timeSeries.getMetricKey().equals(expectedKey2));
    }

    @ParameterizedTest
    @MethodSource("getCommonMetricDifferentUnitTypes")
    public void testHistogramMetricKey(String unit, String expectedUnitName) throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(10);
        Histogram histogram = createHistogramMetric(name, unit);
        PrometheusTimeSeries timeSeries = new PrometheusTimeSeries(histogram, true, Collections.emptyMap());
        final String sanitizedName = PrometheusTimeSeries.sanitizeMetricName(histogram);

        String expectedKey1 = "__name__ "+sanitizedName+"_count "+TEST_ATTR_MAP_STR1+TEST_RESOURCE_MAP_STR+TEST_SCOPE_MAP_STR;
        String expectedKey2 = "__name__ "+sanitizedName+"_count "+TEST_ATTR_MAP_STR2+TEST_RESOURCE_MAP_STR+TEST_SCOPE_MAP_STR;
        assertTrue(timeSeries.getMetricKey().equals(expectedKey1) || timeSeries.getMetricKey().equals(expectedKey2));
    }

    @Test
    public void testSumMetricWithNullAggregationTemporality() {
        final String name = RandomStringUtils.randomAlphabetic(10);
        Sum sum = createSumMetric(name, "s", true, null);
        String sanitizedName = PrometheusTimeSeries.sanitizeMetricName(sum);
        assertThat(sanitizedName, equalTo(name + "_seconds"));
    }

    @Test
    public void testGaugeMetricWithNullUnit() {
        final String name = RandomStringUtils.randomAlphabetic(10);
        Gauge gauge = createGaugeMetric(name, null);
        String sanitizedName = PrometheusTimeSeries.sanitizeMetricName(gauge);
        assertThat(sanitizedName, equalTo(name));
    }

    @Test
    public void testSumMetricCounterWithNullUnit() {
        final String name = RandomStringUtils.randomAlphabetic(10);
        Sum sum = createSumMetric(name, null, true, "AGGREGATION_TEMPORALITY_CUMULATIVE");
        String sanitizedName = PrometheusTimeSeries.sanitizeMetricName(sum);
        assertThat(sanitizedName, equalTo(name + "_total"));
    }

    @Test
    public void testSumMetricWithNullUnitAndNullAggregationTemporality() {
        final String name = RandomStringUtils.randomAlphabetic(10);
        Sum sum = createSumMetric(name, null, true, null);
        String sanitizedName = PrometheusTimeSeries.sanitizeMetricName(sum);
        assertThat(sanitizedName, equalTo(name));
    }

    @Test
    public void testExternalLabelsAreAddedToEveryTimeSeries() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(10);
        final Histogram histogram = createHistogramMetric(name, "s");
        final PrometheusTimeSeries timeSeries = new PrometheusTimeSeries(histogram, true, Map.of("dp_instance", "replica-a"));

        assertThat(timeSeries.getTimeSeriesList().size(), greaterThan(1));
        for (final TimeSeries series : timeSeries.getTimeSeriesList()) {
            assertThat(getLabels(series).get("dp_instance"), equalTo("replica-a"));
        }
    }

    @Test
    public void testExternalLabelsDoNotOverrideMetricAttributes() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(10);
        final Gauge gauge = createGaugeMetric(name, "s");
        final PrometheusTimeSeries timeSeries = new PrometheusTimeSeries(gauge, true, Map.of("attrKey1", "externalValue"));

        final TimeSeries series = timeSeries.getTimeSeriesList().get(0);
        assertThat(getLabels(series).get("attrKey1"), equalTo("1"));
        assertThat(series.getLabelsList().stream().filter(label -> label.getName().equals("attrKey1")).count(), equalTo(1L));
    }

    @Test
    public void testExternalLabelsMakeTheMetricKeyUniquePerInstance() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(10);
        final Gauge gauge = createGaugeMetric(name, "s");

        final PrometheusTimeSeries replicaA = new PrometheusTimeSeries(gauge, true, Map.of("dp_instance", "replica-a"));
        final PrometheusTimeSeries replicaB = new PrometheusTimeSeries(gauge, true, Map.of("dp_instance", "replica-b"));
        final PrometheusTimeSeries withoutExternalLabels = new PrometheusTimeSeries(gauge, true, Collections.emptyMap());

        assertThat(replicaA.getMetricKey(), not(equalTo(replicaB.getMetricKey())));
        assertThat(replicaA.getMetricKey(), not(equalTo(withoutExternalLabels.getMetricKey())));
    }

    @Test
    public void testExternalLabelsNeverRepeatALabelNameTheSinkAddsItself() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(10);
        // An exponential histogram is the only metric which emits "ge", and it emits "le" and "__name__" too.
        final ExponentialHistogram histogram = createExponentialHistogramMetric(name, "s");
        final Map<String, String> reservedNames = PrometheusTimeSeries.RESERVED_LABEL_NAMES.stream()
                .collect(Collectors.toMap(labelName -> labelName, labelName -> "externalValue"));

        final PrometheusTimeSeries timeSeries = new PrometheusTimeSeries(histogram, true, reservedNames);

        assertThat(timeSeries.getTimeSeriesList().size(), greaterThan(1));
        for (final TimeSeries series : timeSeries.getTimeSeriesList()) {
            final List<String> labelNames = series.getLabelsList().stream()
                    .map(Label::getName).collect(Collectors.toList());
            assertThat(labelNames.size(), equalTo(new HashSet<>(labelNames).size()));
        }
    }

    @Test
    public void testNullExternalLabelsAddNoLabels() throws Exception {
        final String name = RandomStringUtils.randomAlphabetic(10);
        final Gauge gauge = createGaugeMetric(name, "s");

        final PrometheusTimeSeries withNullLabels = new PrometheusTimeSeries(gauge, true, null);
        final PrometheusTimeSeries withEmptyLabels = new PrometheusTimeSeries(gauge, true, Collections.emptyMap());

        assertThat(withNullLabels.getMetricKey(), equalTo(withEmptyLabels.getMetricKey()));
    }

    private Map<String, String> getLabels(final TimeSeries series) {
        return series.getLabelsList().stream().collect(Collectors.toMap(Label::getName, Label::getValue));
    }

    private Summary createSummaryMetric(final String name, final String unit) {
        List<Quantile> quantiles = Arrays.asList(
            new DefaultQuantile(0.5d, 10d),
            new DefaultQuantile(0.75d, 20d),
            new DefaultQuantile(0.9d, 30d),
            new DefaultQuantile(0.99d, 5d)
        );
        return JacksonSummary.builder()
            .withName(name)
            .withDescription("Test Summary Metric")
            .withTimeReceived(Instant.now())
            .withTime(Instant.now().plusSeconds(10).toString())
            .withStartTime(Instant.now().plusSeconds(5).toString())
            .withUnit(unit)
            .withSum(50d)
            .withCount(10L)
            .withQuantilesValueCount(4)
            .withQuantiles(quantiles)
            .withAttributes(TEST_ATTR_MAP)
            .withScope(TEST_SCOPE_MAP)
            .withResource(TEST_RESOURCE_MAP)
            .build(false);
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
            .withAttributes(TEST_ATTR_MAP)
            .withScope(TEST_SCOPE_MAP)
            .withResource(TEST_RESOURCE_MAP)
            .build(false);
    }

    private ExponentialHistogram createExponentialHistogramMetric(final String name, final String unit) {
        int scale = 1;
        final List<Long> TEST_POSITIVE_COUNTS = Arrays.asList(1L, 3L, 5L);
        final List<Long> TEST_NEGATIVE_COUNTS = Arrays.asList(4L, 8L, 2L, 6L);
        return JacksonExponentialHistogram.builder()
            .withName(name)
            .withDescription("Test Exponential Histogram Metric")
            .withTimeReceived(Instant.now())
            .withTime(Instant.now().plusSeconds(10).toString())
            .withStartTime(Instant.now().plusSeconds(5).toString())
            .withUnit(unit)
            .withSum(50d)
            .withCount(10)
            .withScale(scale)
            .withPositiveOffset(-1)
            .withNegativeOffset(2)
            .withZeroCount(3)
            .withAggregationTemporality("cumulative")
            .withPositive(TEST_POSITIVE_COUNTS)
            .withNegative(TEST_NEGATIVE_COUNTS)
            .withAttributes(TEST_ATTR_MAP)
            .withScope(TEST_SCOPE_MAP)
            .withResource(TEST_RESOURCE_MAP)
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
            .withAttributes(TEST_ATTR_MAP)
            .withScope(TEST_SCOPE_MAP)
            .withResource(TEST_RESOURCE_MAP)
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
            .withAttributes(TEST_ATTR_MAP)
            .withScope(TEST_SCOPE_MAP)
            .withResource(TEST_RESOURCE_MAP)
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
