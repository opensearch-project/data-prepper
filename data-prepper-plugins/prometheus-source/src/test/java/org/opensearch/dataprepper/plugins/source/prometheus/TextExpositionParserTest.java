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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.metric.Gauge;
import org.opensearch.dataprepper.model.metric.Histogram;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.metric.Quantile;
import org.opensearch.dataprepper.model.metric.Sum;
import org.opensearch.dataprepper.model.metric.Summary;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;

class TextExpositionParserTest {

    private TextExpositionParser parser;

    @BeforeEach
    void setUp() {
        parser = new TextExpositionParser(false);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void testEmptyBodyReturnsEmptyList(final String input) {
        final List<Record<Event>> results = parser.parse(input);
        assertThat(results, hasSize(0));
    }

    @Test
    void testParseGaugeMetric() {
        final String body = "# TYPE cpu_temperature gauge\n" +
                "cpu_temperature{host=\"server1\",region=\"us-east\"} 72.5\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Event event = results.get(0).getData();
        assertThat(event, instanceOf(Gauge.class));

        final Gauge gauge = (Gauge) event;
        assertThat(gauge.getName(), equalTo("cpu_temperature"));
        assertThat(gauge.getValue(), closeTo(72.5, 0.001));

        final Map<String, Object> attributes = gauge.getAttributes();
        assertThat(attributes, hasEntry("host", "server1"));
        assertThat(attributes, hasEntry("region", "us-east"));
    }

    @Test
    void testParseCounterMetric() {
        final String body = "# TYPE http_requests counter\n" +
                "http_requests_total{method=\"POST\",status=\"200\"} 1027\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Event event = results.get(0).getData();
        assertThat(event, instanceOf(Sum.class));

        final Sum sum = (Sum) event;
        assertThat(sum.getName(), equalTo("http_requests"));
        assertThat(sum.getValue(), closeTo(1027.0, 0.001));
        assertThat(sum.isMonotonic(), equalTo(true));
        assertThat(sum.getAggregationTemporality(), equalTo("AGGREGATION_TEMPORALITY_CUMULATIVE"));

        final Map<String, Object> attributes = sum.getAttributes();
        assertThat(attributes, hasEntry("method", "POST"));
        assertThat(attributes, hasEntry("status", "200"));
    }

    @Test
    void testParseHistogramMetric() {
        final String body = "# TYPE request_duration histogram\n" +
                "request_duration_bucket{method=\"GET\",le=\"0.1\"} 10\n" +
                "request_duration_bucket{method=\"GET\",le=\"0.5\"} 50\n" +
                "request_duration_bucket{method=\"GET\",le=\"1.0\"} 80\n" +
                "request_duration_bucket{method=\"GET\",le=\"+Inf\"} 100\n" +
                "request_duration_sum{method=\"GET\"} 25.5\n" +
                "request_duration_count{method=\"GET\"} 100\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Event event = results.get(0).getData();
        assertThat(event, instanceOf(Histogram.class));

        final Histogram histogram = (Histogram) event;
        assertThat(histogram.getName(), equalTo("request_duration"));
        assertThat(histogram.getSum(), closeTo(25.5, 0.001));
        assertThat(histogram.getCount(), equalTo(100L));
        assertThat(histogram.getAggregationTemporality(), equalTo("AGGREGATION_TEMPORALITY_CUMULATIVE"));

        final List<Double> explicitBounds = histogram.getExplicitBoundsList();
        assertThat(explicitBounds, hasSize(3));
        assertThat(explicitBounds.get(0), closeTo(0.1, 0.001));
        assertThat(explicitBounds.get(1), closeTo(0.5, 0.001));
        assertThat(explicitBounds.get(2), closeTo(1.0, 0.001));

        final List<Long> bucketCounts = histogram.getBucketCountsList();
        assertThat(bucketCounts, hasSize(4));
        assertThat(bucketCounts.get(0), equalTo(10L));
        assertThat(bucketCounts.get(1), equalTo(40L));
        assertThat(bucketCounts.get(2), equalTo(30L));
        assertThat(bucketCounts.get(3), equalTo(20L));

        assertThat(histogram.getBucketCount(), equalTo(4));
        assertThat(histogram.getExplicitBoundsCount(), equalTo(3));

        final Map<String, Object> attributes = histogram.getAttributes();
        assertThat(attributes, hasEntry("method", "GET"));
    }

    @Test
    void testParseSummaryMetric() {
        final String body = "# TYPE rpc_latency summary\n" +
                "rpc_latency{service=\"api\",quantile=\"0.5\"} 0.2\n" +
                "rpc_latency{service=\"api\",quantile=\"0.99\"} 0.8\n" +
                "rpc_latency_sum{service=\"api\"} 300.5\n" +
                "rpc_latency_count{service=\"api\"} 1000\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Event event = results.get(0).getData();
        assertThat(event, instanceOf(Summary.class));

        final Summary summary = (Summary) event;
        assertThat(summary.getName(), equalTo("rpc_latency"));
        assertThat(summary.getSum(), closeTo(300.5, 0.001));
        assertThat(summary.getCount(), equalTo(1000L));
        assertThat(summary.getQuantileValuesCount(), equalTo(2));

        final List<? extends Quantile> quantiles = summary.getQuantiles();
        assertThat(quantiles, hasSize(2));
        assertThat(quantiles.get(0).getQuantile(), closeTo(0.5, 0.001));
        assertThat(quantiles.get(0).getValue(), closeTo(0.2, 0.001));
        assertThat(quantiles.get(1).getQuantile(), closeTo(0.99, 0.001));
        assertThat(quantiles.get(1).getValue(), closeTo(0.8, 0.001));

        final Map<String, Object> attributes = summary.getAttributes();
        assertThat(attributes, hasEntry("service", "api"));
    }

    @Test
    void testParseUntypedMetric() {
        final String body = "some_untyped_metric{env=\"prod\"} 42\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Event event = results.get(0).getData();
        assertThat(event, instanceOf(Gauge.class));

        final Gauge gauge = (Gauge) event;
        assertThat(gauge.getName(), equalTo("some_untyped_metric"));
        assertThat(gauge.getValue(), closeTo(42.0, 0.001));
    }

    @Test
    void testParseMultipleMetricTypes() {
        final String body = "# TYPE temp gauge\n" +
                "temp{loc=\"a\"} 22.5\n" +
                "# TYPE reqs counter\n" +
                "reqs_total{path=\"/\"} 500\n" +
                "# TYPE latency histogram\n" +
                "latency_bucket{le=\"0.5\"} 20\n" +
                "latency_bucket{le=\"+Inf\"} 50\n" +
                "latency_sum 15.0\n" +
                "latency_count 50\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(3));

        assertThat(results.get(0).getData(), instanceOf(Gauge.class));
        assertThat(results.get(1).getData(), instanceOf(Sum.class));
        assertThat(results.get(2).getData(), instanceOf(Histogram.class));

        final Gauge gauge = (Gauge) results.get(0).getData();
        assertThat(gauge.getName(), equalTo("temp"));

        final Sum sum = (Sum) results.get(1).getData();
        assertThat(sum.getName(), equalTo("reqs"));

        final Histogram histogram = (Histogram) results.get(2).getData();
        assertThat(histogram.getName(), equalTo("latency"));
    }

    @Test
    void testParseWithMissingTimestamp() {
        final String body = "# TYPE my_gauge gauge\n" +
                "my_gauge 99.9\n";

        final Instant before = Instant.now();
        final List<Record<Event>> results = parser.parse(body);
        final Instant after = Instant.now();

        assertThat(results, hasSize(1));
        final Metric metric = (Metric) results.get(0).getData();
        final String timeStr = metric.getTime();
        assertThat(timeStr, notNullValue());

        final Instant parsedTime = Instant.parse(timeStr);
        assertThat(parsedTime.toEpochMilli(), greaterThan(before.toEpochMilli() - 1000));
    }

    @Test
    void testParseWithTimestamp() {
        final String body = "# TYPE my_gauge gauge\n" +
                "my_gauge{host=\"a\"} 55.5 1625000000000\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Metric metric = (Metric) results.get(0).getData();
        final String timeStr = metric.getTime();
        final Instant expectedTime = Instant.ofEpochMilli(1625000000000L);
        assertThat(timeStr, equalTo(expectedTime.toString()));
    }

    @Test
    void testNaNValue() {
        final String body = "# TYPE my_gauge gauge\n" +
                "my_gauge NaN\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Gauge gauge = (Gauge) results.get(0).getData();
        assertThat(Double.isNaN(gauge.getValue()), equalTo(true));
    }

    @Test
    void testPositiveInfValue() {
        final String body = "# TYPE my_gauge gauge\n" +
                "my_gauge +Inf\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Gauge gauge = (Gauge) results.get(0).getData();
        assertThat(Double.isInfinite(gauge.getValue()), equalTo(true));
        assertThat(gauge.getValue(), greaterThan(0.0));
    }

    @Test
    void testNegativeInfValue() {
        final String body = "# TYPE my_gauge gauge\n" +
                "my_gauge -Inf\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Gauge gauge = (Gauge) results.get(0).getData();
        assertThat(Double.isInfinite(gauge.getValue()), equalTo(true));
        assertThat(gauge.getValue(), equalTo(Double.NEGATIVE_INFINITY));
    }

    @Test
    void testServiceNameExtraction() {
        final String body = "# TYPE my_metric gauge\n" +
                "my_metric{job=\"my-service\",instance=\"localhost:9090\"} 1.0\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Metric metric = (Metric) results.get(0).getData();
        assertThat(metric.getServiceName(), equalTo("my-service"));
    }

    @Test
    void testServiceNameExtractionFromServiceNameLabel() {
        final String body = "# TYPE my_metric gauge\n" +
                "my_metric{service_name=\"explicit-svc\"} 1.0\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Metric metric = (Metric) results.get(0).getData();
        assertThat(metric.getServiceName(), equalTo("explicit-svc"));
    }

    @Test
    void testFlattenLabels() {
        final TextExpositionParser flatParser = new TextExpositionParser(true);
        final String body = "# TYPE my_gauge gauge\n" +
                "my_gauge{host=\"server1\",region=\"us-west\"} 42.0\n";

        final List<Record<Event>> results = flatParser.parse(body);

        assertThat(results, hasSize(1));
        final Event event = results.get(0).getData();
        assertThat(event, instanceOf(Gauge.class));
        final Gauge gauge = (Gauge) event;
        assertThat(gauge.getValue(), closeTo(42.0, 0.001));
    }

    @Test
    void testCommentAndBlankLinesIgnored() {
        final String body = "# HELP my_gauge A helpful description.\n" +
                "# TYPE my_gauge gauge\n" +
                "\n" +
                "   \n" +
                "# This is a comment\n" +
                "my_gauge 10.0\n" +
                "\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Gauge gauge = (Gauge) results.get(0).getData();
        assertThat(gauge.getName(), equalTo("my_gauge"));
        assertThat(gauge.getValue(), closeTo(10.0, 0.001));
    }

    @Test
    void testParseLabelsWithEscapedQuotes() {
        final String body = "# TYPE my_metric gauge\n" +
                "my_metric{label=\"value with \\\"quotes\\\"\"} 5.0\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Gauge gauge = (Gauge) results.get(0).getData();
        assertThat(gauge.getValue(), closeTo(5.0, 0.001));

        final Map<String, Object> attributes = gauge.getAttributes();
        assertThat(attributes, hasEntry("label", "value with \"quotes\""));
    }

    @Test
    void testParseLabelsWithEscapedBackslash() {
        final String body = "# TYPE my_metric gauge\n" +
                "my_metric{path=\"C:\\\\Users\\\\test\"} 3.0\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Map<String, Object> attributes = ((Gauge) results.get(0).getData()).getAttributes();
        assertThat(attributes, hasEntry("path", "C:\\Users\\test"));
    }

    @Test
    void testParseLabelsWithEscapedNewline() {
        final String body = "# TYPE my_metric gauge\n" +
                "my_metric{msg=\"line1\\nline2\"} 7.0\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Map<String, Object> attributes = ((Gauge) results.get(0).getData()).getAttributes();
        assertThat(attributes, hasEntry("msg", "line1\nline2"));
    }

    @Test
    void testCounterWithoutTotalSuffix() {
        final String body = "# TYPE my_counter counter\n" +
                "my_counter{env=\"test\"} 100\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Sum sum = (Sum) results.get(0).getData();
        assertThat(sum.getName(), equalTo("my_counter"));
        assertThat(sum.getValue(), closeTo(100.0, 0.001));
    }

    @Test
    void testCounterWithCreatedSuffix() {
        final String body = "# TYPE my_counter counter\n" +
                "my_counter_created{env=\"test\"} 1625000000\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(0));
    }

    @Test
    void testHistogramWithNoBuckets() {
        final String body = "# TYPE empty_histogram histogram\n" +
                "empty_histogram_sum 0\n" +
                "empty_histogram_count 0\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(0));
    }

    @Test
    void testSummaryWithNoQuantiles() {
        final String body = "# TYPE empty_summary summary\n" +
                "empty_summary_sum 0\n" +
                "empty_summary_count 0\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(0));
    }

    @Test
    void testMetricWithNoLabels() {
        final String body = "# TYPE simple_gauge gauge\n" +
                "simple_gauge 123.456\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Gauge gauge = (Gauge) results.get(0).getData();
        assertThat(gauge.getName(), equalTo("simple_gauge"));
        assertThat(gauge.getValue(), closeTo(123.456, 0.001));
    }

    @Test
    void testResolveTypeWithDeclaredType() {
        final String body = "# TYPE my_metric untyped\n" +
                "my_metric 5.0\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        assertThat(results.get(0).getData(), instanceOf(Gauge.class));
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "# TYPE test_metric gauge",
            "# TYPE test_metric counter",
            "# TYPE test_metric histogram",
            "# TYPE test_metric summary",
            "# TYPE test_metric untyped"
    })
    void testParseTypeLineWithVariousTypes(final String typeLine) {
        final Map<String, String> declaredTypes = new HashMap<>();
        parser.parseTypeLine(typeLine, declaredTypes);
        assertThat(declaredTypes.size(), equalTo(1));
        assertThat(declaredTypes.containsKey("test_metric"), equalTo(true));
    }

    @Test
    void testParseTypeLineWithInvalidLine() {
        final Map<String, String> declaredTypes = new HashMap<>();
        parser.parseTypeLine("# TYPE nospace", declaredTypes);
        assertThat(declaredTypes.size(), equalTo(0));
    }

    @Test
    void testResolveTypeForSuffixedNames() {
        final Map<String, String> declaredTypes = new HashMap<>();
        declaredTypes.put("base_metric", "histogram");

        assertThat(parser.resolveType("base_metric_bucket", declaredTypes), equalTo("histogram"));
        assertThat(parser.resolveType("base_metric_count", declaredTypes), equalTo("histogram"));
        assertThat(parser.resolveType("base_metric_sum", declaredTypes), equalTo("histogram"));
        assertThat(parser.resolveType("unknown_metric", declaredTypes), equalTo("gauge"));
    }

    @Test
    void testDeriveHistogramBaseName() {
        assertThat(parser.deriveHistogramBaseName("request_duration_bucket"), equalTo("request_duration"));
        assertThat(parser.deriveHistogramBaseName("request_duration_count"), equalTo("request_duration"));
        assertThat(parser.deriveHistogramBaseName("request_duration_sum"), equalTo("request_duration"));
        assertThat(parser.deriveHistogramBaseName("request_duration"), equalTo("request_duration"));
    }

    @Test
    void testDeriveSummaryBaseName() {
        assertThat(parser.deriveSummaryBaseName("rpc_latency_count"), equalTo("rpc_latency"));
        assertThat(parser.deriveSummaryBaseName("rpc_latency_sum"), equalTo("rpc_latency"));
        assertThat(parser.deriveSummaryBaseName("rpc_latency"), equalTo("rpc_latency"));
    }

    @Test
    void testBuildSortedLabelKey() {
        final Map<String, String> labels = new LinkedHashMap<>();
        labels.put("z_key", "z_val");
        labels.put("a_key", "a_val");

        final String result = TextExpositionParser.buildSortedLabelKey(labels);
        assertThat(result, equalTo("a_key;a_val;z_key;z_val"));
    }

    @Test
    void testBuildSortedLabelKeyEmpty() {
        final String result = TextExpositionParser.buildSortedLabelKey(Collections.emptyMap());
        assertThat(result, equalTo(""));
    }

    @Test
    void testExtractServiceNamePriority() {
        final Map<String, Object> attrs = new HashMap<>();
        attrs.put("service.name", "svc-dot");
        attrs.put("service_name", "svc-underscore");
        attrs.put("job", "svc-job");
        assertThat(TextExpositionParser.extractServiceName(attrs), equalTo("svc-dot"));

        final Map<String, Object> attrsNoServiceName = new HashMap<>();
        attrsNoServiceName.put("service_name", "svc-underscore");
        attrsNoServiceName.put("job", "svc-job");
        assertThat(TextExpositionParser.extractServiceName(attrsNoServiceName), equalTo("svc-underscore"));

        final Map<String, Object> attrsJobOnly = new HashMap<>();
        attrsJobOnly.put("job", "svc-job");
        assertThat(TextExpositionParser.extractServiceName(attrsJobOnly), equalTo("svc-job"));

        final Map<String, Object> attrsEmpty = new HashMap<>();
        assertThat(TextExpositionParser.extractServiceName(attrsEmpty), equalTo(""));
    }

    @Test
    void testStripCounterSuffix() {
        assertThat(TextExpositionParser.stripCounterSuffix("http_requests_total"), equalTo("http_requests"));
        assertThat(TextExpositionParser.stripCounterSuffix("http_requests_created"), equalTo("http_requests"));
        assertThat(TextExpositionParser.stripCounterSuffix("http_requests"), equalTo("http_requests"));
    }

    @Test
    void testResolveTimestampWithNull() {
        final Instant timeReceived = Instant.now();
        final String result = TextExpositionParser.resolveTimestamp(null, timeReceived);
        assertThat(result, equalTo(timeReceived.toString()));
    }

    @Test
    void testResolveTimestampWithValue() {
        final Instant timeReceived = Instant.now();
        final String result = TextExpositionParser.resolveTimestamp(1625000000000L, timeReceived);
        assertThat(result, equalTo(Instant.ofEpochMilli(1625000000000L).toString()));
    }

    @Test
    void testParseValueSpecialValues() {
        assertThat(Double.isNaN(TextExpositionParser.parseValue("NaN")), equalTo(true));
        assertThat(TextExpositionParser.parseValue("+Inf"), equalTo(Double.POSITIVE_INFINITY));
        assertThat(TextExpositionParser.parseValue("-Inf"), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(TextExpositionParser.parseValue("42.5"), closeTo(42.5, 0.001));
    }

    @Test
    void testParseLeValue() {
        assertThat(TextExpositionParser.parseLeValue(null), equalTo(null));
        assertThat(TextExpositionParser.parseLeValue("+Inf"), equalTo(Double.POSITIVE_INFINITY));
        assertThat(TextExpositionParser.parseLeValue("-Inf"), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(TextExpositionParser.parseLeValue("0.5"), closeTo(0.5, 0.001));
        assertThat(TextExpositionParser.parseLeValue("not_a_number"), equalTo(null));
    }

    @Test
    void testParseQuantileValue() {
        assertThat(TextExpositionParser.parseQuantileValue(null), equalTo(null));
        assertThat(TextExpositionParser.parseQuantileValue("0.99"), closeTo(0.99, 0.001));
        assertThat(TextExpositionParser.parseQuantileValue("bad_value"), equalTo(null));
    }

    @Test
    void testParseSampleLineWithEmptyLabels() {
        final TextExpositionParser.ParsedSample sample = parser.parseSampleLine("metric_name{} 42.0");
        assertThat(sample, notNullValue());
        assertThat(sample.name, equalTo("metric_name"));
        assertThat(sample.value, closeTo(42.0, 0.001));
        assertThat(sample.labels.size(), equalTo(0));
    }

    @Test
    void testParseSampleLineWithTimestamp() {
        final TextExpositionParser.ParsedSample sample = parser.parseSampleLine("metric_name 42.0 1625000000000");
        assertThat(sample, notNullValue());
        assertThat(sample.name, equalTo("metric_name"));
        assertThat(sample.value, closeTo(42.0, 0.001));
        assertThat(sample.timestampMs, equalTo(1625000000000L));
    }

    @Test
    void testParseSampleLineWithNoTimestamp() {
        final TextExpositionParser.ParsedSample sample = parser.parseSampleLine("metric_name 42.0");
        assertThat(sample, notNullValue());
        assertThat(sample.timestampMs, equalTo(null));
    }

    @Test
    void testParseSampleLineInvalid() {
        final TextExpositionParser.ParsedSample sample = parser.parseSampleLine("");
        assertThat(sample, equalTo(null));
    }

    @Test
    void testParseLabelsMethod() {
        final Map<String, String> labels = new LinkedHashMap<>();
        final String line = "metric{key1=\"val1\",key2=\"val2\"} 1.0";
        final int endIdx = parser.parseLabels(line, 6, labels);

        assertThat(labels.size(), equalTo(2));
        assertThat(labels.get("key1"), equalTo("val1"));
        assertThat(labels.get("key2"), equalTo("val2"));
        assertThat(endIdx, greaterThan(6));
    }

    @Test
    void testHistogramCumulativeToPerBucketConversion() {
        final String body = "# TYPE latency histogram\n" +
                "latency_bucket{le=\"1.0\"} 5\n" +
                "latency_bucket{le=\"5.0\"} 15\n" +
                "latency_bucket{le=\"10.0\"} 15\n" +
                "latency_bucket{le=\"+Inf\"} 20\n" +
                "latency_sum 50.0\n" +
                "latency_count 20\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Histogram histogram = (Histogram) results.get(0).getData();
        final List<Long> bucketCounts = histogram.getBucketCountsList();
        assertThat(bucketCounts.get(0), equalTo(5L));
        assertThat(bucketCounts.get(1), equalTo(10L));
        assertThat(bucketCounts.get(2), equalTo(0L));
        assertThat(bucketCounts.get(3), equalTo(5L));
    }

    @Test
    void testMultipleHistogramGroups() {
        final String body = "# TYPE request_duration histogram\n" +
                "request_duration_bucket{method=\"GET\",le=\"1.0\"} 10\n" +
                "request_duration_bucket{method=\"GET\",le=\"+Inf\"} 20\n" +
                "request_duration_sum{method=\"GET\"} 5.0\n" +
                "request_duration_count{method=\"GET\"} 20\n" +
                "request_duration_bucket{method=\"POST\",le=\"1.0\"} 3\n" +
                "request_duration_bucket{method=\"POST\",le=\"+Inf\"} 8\n" +
                "request_duration_sum{method=\"POST\"} 2.5\n" +
                "request_duration_count{method=\"POST\"} 8\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(2));
        assertThat(results.get(0).getData(), instanceOf(Histogram.class));
        assertThat(results.get(1).getData(), instanceOf(Histogram.class));
    }

    @Test
    void testParseSampleLineWithUnparseableTimestamp() {
        final TextExpositionParser.ParsedSample sample = parser.parseSampleLine("metric_name 42.0 not_a_timestamp");
        assertThat(sample, notNullValue());
        assertThat(sample.value, closeTo(42.0, 0.001));
        assertThat(sample.timestampMs, equalTo(null));
    }

    @Test
    void testParseSampleLineWithNoValue() {
        final TextExpositionParser.ParsedSample sample = parser.parseSampleLine("metric_name{key=\"val\"}");
        assertThat(sample, equalTo(null));
    }

    @Test
    void testParseLabelsWithWhitespaceAroundLabels() {
        final Map<String, String> labels = new LinkedHashMap<>();
        final String line = "metric{ key1=\"val1\" } 1.0";
        parser.parseLabels(line, 6, labels);
        assertThat(labels.size(), equalTo(1));
        assertThat(labels.get("key1"), equalTo("val1"));
    }

    @Test
    void testParseLabelsWithMalformedNoEquals() {
        final Map<String, String> labels = new LinkedHashMap<>();
        final String line = "metric{malformed_label} 1.0";
        parser.parseLabels(line, 6, labels);
        assertThat(labels.size(), equalTo(0));
    }

    @Test
    void testParseLabelsWithMissingQuoteAfterEquals() {
        final Map<String, String> labels = new LinkedHashMap<>();
        final String line = "metric{key=noquote} 1.0";
        parser.parseLabels(line, 6, labels);
        assertThat(labels.size(), equalTo(0));
    }

    @Test
    void testParseLabelsWithUnterminatedValue() {
        final Map<String, String> labels = new LinkedHashMap<>();
        final String line = "metric{key=\"unterminated";
        parser.parseLabels(line, 6, labels);
        assertThat(labels.size(), equalTo(1));
        assertThat(labels.get("key"), equalTo("unterminated"));
    }

    @Test
    void testParseLabelsWithBackslashAtEnd() {
        final Map<String, String> labels = new LinkedHashMap<>();
        final String line = "metric{key=\"val\\\"} 1.0";
        parser.parseLabels(line, 6, labels);
        assertThat(labels.containsKey("key"), equalTo(true));
    }

    @Test
    void testHistogramWithNonMonotonicBucketsClampedToZero() {
        final String body = "# TYPE broken histogram\n" +
                "broken_bucket{le=\"1.0\"} 100\n" +
                "broken_bucket{le=\"2.0\"} 50\n" +
                "broken_bucket{le=\"+Inf\"} 150\n" +
                "broken_sum 10.0\n" +
                "broken_count 150\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Histogram histogram = (Histogram) results.get(0).getData();
        final List<Long> bucketCounts = histogram.getBucketCountsList();
        assertThat(bucketCounts.get(0), equalTo(100L));
        assertThat(bucketCounts.get(1), equalTo(0L));
        assertThat(bucketCounts.get(2), equalTo(50L));
    }

    @Test
    void testResolveTypeWithTotalSuffix() {
        final Map<String, String> declaredTypes = new HashMap<>();
        declaredTypes.put("requests", "counter");
        assertThat(parser.resolveType("requests_total", declaredTypes), equalTo("counter"));
    }

    @Test
    void testResolveTypeWithCreatedSuffix() {
        final Map<String, String> declaredTypes = new HashMap<>();
        declaredTypes.put("requests", "counter");
        assertThat(parser.resolveType("requests_created", declaredTypes), equalTo("counter"));
    }

    @Test
    void testParseLabelsAtEndOfString() {
        final Map<String, String> labels = new LinkedHashMap<>();
        final String line = "metric{key=\"val\"";
        final int endIdx = parser.parseLabels(line, 6, labels);
        assertThat(labels.size(), equalTo(1));
        assertThat(labels.get("key"), equalTo("val"));
    }

    @Test
    void testParseLabelsWithUnknownEscapeSequence() {
        final Map<String, String> labels = new LinkedHashMap<>();
        final String line = "metric{key=\"val\\xend\"} 1.0";
        parser.parseLabels(line, 6, labels);
        assertThat(labels.size(), equalTo(1));
        assertThat(labels.get("key"), equalTo("val\\xend"));
    }

    @Test
    void testHistogramSampleWithBareName() {
        final String body = "# TYPE my_hist histogram\n" +
                "my_hist_bucket{le=\"1.0\"} 5\n" +
                "my_hist_bucket{le=\"+Inf\"} 10\n" +
                "my_hist_sum 5.0\n" +
                "my_hist_count 10\n" +
                "my_hist 99\n";

        final List<Record<Event>> results = parser.parse(body);
        assertThat(results, hasSize(1));
        assertThat(results.get(0).getData(), instanceOf(Histogram.class));
    }

    @Test
    void testSummarySampleWithBareName() {
        final String body = "# TYPE my_summary summary\n" +
                "my_summary{quantile=\"0.5\"} 0.1\n" +
                "my_summary_sum 10.0\n" +
                "my_summary_count 100\n" +
                "my_summary 42\n";

        final List<Record<Event>> results = parser.parse(body);
        assertThat(results, hasSize(1));
        assertThat(results.get(0).getData(), instanceOf(Summary.class));
    }

    @Test
    void testResolveTypeDirectMatch() {
        final Map<String, String> declaredTypes = new HashMap<>();
        declaredTypes.put("my_metric", "gauge");
        assertThat(parser.resolveType("my_metric", declaredTypes), equalTo("gauge"));
    }

    @Test
    void testParseSampleLineWithUnparseableValue() {
        final TextExpositionParser.ParsedSample sample = parser.parseSampleLine("metric_name not_a_number");
        assertThat(sample, equalTo(null));
    }

    @Test
    void testServiceNameEmptyWhenNoJobLabel() {
        final String body = "# TYPE simple gauge\n" +
                "simple{instance=\"localhost\"} 1.0\n";

        final List<Record<Event>> results = parser.parse(body);

        assertThat(results, hasSize(1));
        final Metric metric = (Metric) results.get(0).getData();
        assertThat(metric.getServiceName(), equalTo(""));
    }
}