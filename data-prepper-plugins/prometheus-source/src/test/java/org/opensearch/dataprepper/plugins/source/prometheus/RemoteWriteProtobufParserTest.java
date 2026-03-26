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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.metric.Gauge;
import org.opensearch.dataprepper.model.metric.Histogram;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.metric.Sum;
import org.opensearch.dataprepper.model.metric.Summary;
import org.opensearch.dataprepper.model.record.Record;
import org.xerial.snappy.Snappy;
import com.arpnetworking.metrics.prometheus.Remote;
import com.arpnetworking.metrics.prometheus.Types;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.lenient;

@ExtendWith(MockitoExtension.class)
class RemoteWriteProtobufParserTest {

    @Mock
    private PrometheusRemoteWriteSourceConfig config;

    private RemoteWriteProtobufParser parser;

    @BeforeEach
    void setUp() {
        lenient().when(config.isFlattenLabels()).thenReturn(false);
        parser = new RemoteWriteProtobufParser(config);
    }

    @Test
    void testParseSimpleGauge() throws Exception {
        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("cpu_temperature").build())
                .addLabels(Types.Label.newBuilder().setName("host").setValue("server-01").build())
                .addSamples(Types.Sample.newBuilder().setValue(72.5).setTimestamp(1706869800000L).build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().addTimeseries(timeSeries).build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records.size(), equalTo(1));
        final Event event = records.get(0).getData();
        assertThat(event.get("name", String.class), equalTo("cpu_temperature"));
        assertThat(event.get("value", Double.class), equalTo(72.5));
        assertThat(event.get("kind", String.class), equalTo(Metric.KIND.GAUGE.toString()));
        assertThat(event.get("time", String.class), equalTo("2024-02-02T10:30:00Z"));
        assertThat(event, instanceOf(Gauge.class));

        final Map<String, Object> attributes = event.get("attributes", Map.class);
        assertThat(attributes, hasEntry("host", "server-01"));
        assertThat(attributes, not(hasKey("__name__")));
    }

    @Test
    void testParseCounterEmitsSumWithStrippedName() throws Exception {
        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("http_requests_total").build())
                .addLabels(Types.Label.newBuilder().setName("method").setValue("GET").build())
                .addSamples(Types.Sample.newBuilder().setValue(100.0).setTimestamp(1706869800000L).build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().addTimeseries(timeSeries).build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records.size(), equalTo(1));
        final Event event = records.get(0).getData();
        assertThat(event.get("kind", String.class), equalTo(Metric.KIND.SUM.toString()));
        assertThat(event.get("isMonotonic", Boolean.class), equalTo(true));
        assertThat(event.get("name", String.class), equalTo("http_requests"));
        assertThat(event, instanceOf(Sum.class));
    }

    @Test
    void testParseCreatedSuffixEmitsSum() throws Exception {
        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("http_requests_created").build())
                .addSamples(Types.Sample.newBuilder().setValue(1.0).setTimestamp(1706869800000L).build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().addTimeseries(timeSeries).build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records.size(), equalTo(1));
        assertThat(records.get(0).getData().get("kind", String.class), equalTo(Metric.KIND.SUM.toString()));
    }

    @Test
    void testParseMultipleSamples() throws Exception {
        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("request_count").build())
                .addLabels(Types.Label.newBuilder().setName("method").setValue("GET").build())
                .addSamples(Types.Sample.newBuilder().setValue(100.0).setTimestamp(1706869800000L).build())
                .addSamples(Types.Sample.newBuilder().setValue(150.0).setTimestamp(1706869860000L).build())
                .addSamples(Types.Sample.newBuilder().setValue(200.0).setTimestamp(1706869920000L).build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().addTimeseries(timeSeries).build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records.size(), equalTo(3));
        assertThat(records.get(0).getData().get("value", Double.class), equalTo(100.0));
        assertThat(records.get(1).getData().get("value", Double.class), equalTo(150.0));
        assertThat(records.get(2).getData().get("value", Double.class), equalTo(200.0));
    }

    @Test
    void testParseMultipleTimeSeries() throws Exception {
        final Types.TimeSeries ts1 = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("metric_a").build())
                .addSamples(Types.Sample.newBuilder().setValue(10.0).setTimestamp(1706869800000L).build())
                .build();

        final Types.TimeSeries ts2 = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("metric_b").build())
                .addSamples(Types.Sample.newBuilder().setValue(20.0).setTimestamp(1706869800000L).build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().addTimeseries(ts1).addTimeseries(ts2).build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records.size(), equalTo(2));
        assertThat(records.get(0).getData().get("name", String.class), equalTo("metric_a"));
        assertThat(records.get(1).getData().get("name", String.class), equalTo("metric_b"));
    }

    @Test
    void testParseInvalidSnappyData() {
        final byte[] invalidData = "not snappy compressed".getBytes();
        assertThrows(IOException.class, () -> parser.parse(invalidData));
    }

    @Test
    void testParseEmptyWriteRequest() throws Exception {
        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().build().toByteArray());
        assertThat(parser.parse(compressed).size(), equalTo(0));
    }

    @Test
    void testParseWithMultipleLabels() throws Exception {
        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("http_requests_total").build())
                .addLabels(Types.Label.newBuilder().setName("method").setValue("GET").build())
                .addLabels(Types.Label.newBuilder().setName("status").setValue("200").build())
                .addLabels(Types.Label.newBuilder().setName("service").setValue("api").build())
                .addSamples(Types.Sample.newBuilder().setValue(100.0).setTimestamp(1706869800000L).build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().addTimeseries(timeSeries).build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records.size(), equalTo(1));

        @SuppressWarnings("unchecked")
        final Map<String, Object> attributes = records.get(0).getData().get("attributes", Map.class);
        assertThat(attributes, hasEntry("method", "GET"));
        assertThat(attributes, hasEntry("status", "200"));
        assertThat(attributes, hasEntry("service", "api"));
    }

    @Test
    void testParseMissingNameLabelDefaultsToUnknownMetric() throws Exception {
        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("host").setValue("server-01").build())
                .addSamples(Types.Sample.newBuilder().setValue(42.0).setTimestamp(1706869800000L).build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().addTimeseries(timeSeries).build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records.size(), equalTo(1));
        assertThat(records.get(0).getData().get("name", String.class), equalTo("unknown_metric"));
    }

    @Test
    void testParseFlattenLabelsTrue() throws Exception {
        lenient().when(config.isFlattenLabels()).thenReturn(true);
        final RemoteWriteProtobufParser flattenParser = new RemoteWriteProtobufParser(config);

        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("cpu_temp").build())
                .addLabels(Types.Label.newBuilder().setName("host").setValue("server-01").build())
                .addSamples(Types.Sample.newBuilder().setValue(72.5).setTimestamp(1706869800000L).build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().addTimeseries(timeSeries).build().toByteArray());
        final List<Record<Event>> records = flattenParser.parse(compressed);

        assertThat(records.size(), equalTo(1));
        final Event event = records.get(0).getData();
        assertThat(event.get("name", String.class), equalTo("cpu_temp"));
        assertThat(event.get("kind", String.class), equalTo(Metric.KIND.GAUGE.toString()));
        assertThat(event.get("value", Double.class), equalTo(72.5));
    }

    @Test
    void testParseNaNValue() throws Exception {
        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("stale_metric").build())
                .addSamples(Types.Sample.newBuilder().setValue(Double.NaN).setTimestamp(1706869800000L).build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().addTimeseries(timeSeries).build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records.size(), equalTo(1));
        assertThat(Double.isNaN(records.get(0).getData().get("value", Double.class)), equalTo(true));
    }

    @Test
    void testParsePositiveInfinityValue() throws Exception {
        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("bucket_metric").build())
                .addSamples(Types.Sample.newBuilder().setValue(Double.POSITIVE_INFINITY).setTimestamp(1706869800000L).build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().addTimeseries(timeSeries).build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records.size(), equalTo(1));
        assertThat(records.get(0).getData().get("value", Double.class), equalTo(Double.POSITIVE_INFINITY));
    }

    @Test
    void testParseZeroTimestampUsesCurrentTime() throws Exception {
        final Instant beforeParse = Instant.now();

        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("test_metric").build())
                .addSamples(Types.Sample.newBuilder().setValue(1.0).setTimestamp(0L).build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().addTimeseries(timeSeries).build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);
        final Instant afterParse = Instant.now();

        assertThat(records.size(), equalTo(1));
        final String timeStr = records.get(0).getData().get("time", String.class);
        final Instant parsedTime = Instant.parse(timeStr);
        assertThat(parsedTime, greaterThan(beforeParse.minusSeconds(1)));
        assertThat(parsedTime.isBefore(afterParse.plusSeconds(1)), equalTo(true));
    }

    @Test
    void testParseTimeSeriesWithNoSamples() throws Exception {
        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("empty_metric").build())
                .addLabels(Types.Label.newBuilder().setName("host").setValue("server-01").build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().addTimeseries(timeSeries).build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records.size(), equalTo(0));
    }

    @ParameterizedTest
    @ValueSource(strings = {"http_requests_total", "process_cpu_seconds_total", "go_gc_duration_seconds_created"})
    void testIsCounterReturnsTrueForCounterSuffixes(final String metricName) {
        assertThat(RemoteWriteProtobufParser.isCounter(metricName), equalTo(true));
    }

    @ParameterizedTest
    @ValueSource(strings = {"cpu_temperature", "memory_usage_bytes", "http_request_duration_seconds",
            "go_goroutines", "process_resident_memory_bytes"})
    void testIsCounterReturnsFalseForNonCounters(final String metricName) {
        assertThat(RemoteWriteProtobufParser.isCounter(metricName), equalTo(false));
    }

    @ParameterizedTest
    @ValueSource(strings = {"http_request_duration", "api_latency", "db_query_time"})
    void testParseHistogram(final String baseName) throws Exception {
        final String bucketName = baseName + "_bucket";
        final String countName = baseName + "_count";
        final String sumName = baseName + "_sum";

        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue(bucketName).build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("0.1").build())
                .addLabels(Types.Label.newBuilder().setName("method").setValue("GET").build())
                .addSamples(Types.Sample.newBuilder().setValue(5.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue(bucketName).build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("0.5").build())
                .addLabels(Types.Label.newBuilder().setName("method").setValue("GET").build())
                .addSamples(Types.Sample.newBuilder().setValue(10.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue(bucketName).build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("1.0").build())
                .addLabels(Types.Label.newBuilder().setName("method").setValue("GET").build())
                .addSamples(Types.Sample.newBuilder().setValue(15.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue(bucketName).build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("+Inf").build())
                .addLabels(Types.Label.newBuilder().setName("method").setValue("GET").build())
                .addSamples(Types.Sample.newBuilder().setValue(20.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue(countName).build())
                .addLabels(Types.Label.newBuilder().setName("method").setValue("GET").build())
                .addSamples(Types.Sample.newBuilder().setValue(20.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue(sumName).build())
                .addLabels(Types.Label.newBuilder().setName("method").setValue("GET").build())
                .addSamples(Types.Sample.newBuilder().setValue(5.5).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(1));
        final Event event = records.get(0).getData();

        assertThat(event.get("kind", String.class), equalTo(Metric.KIND.HISTOGRAM.toString()));
        assertThat(event.get("name", String.class), equalTo(baseName));
        assertThat(event.get("sum", Double.class), closeTo(5.5, 0.001));
        assertThat(event.get("count", Long.class), equalTo(20L));
        assertThat(event, instanceOf(Histogram.class));

        final List<Long> bucketCounts = event.get("bucketCountsList", List.class);
        assertThat(bucketCounts, hasSize(4));
        assertThat(bucketCounts.get(0), equalTo(5L));
        assertThat(bucketCounts.get(1), equalTo(5L));
        assertThat(bucketCounts.get(2), equalTo(5L));
        assertThat(bucketCounts.get(3), equalTo(5L));

        final List<Double> explicitBounds = event.get("explicitBounds", List.class);
        assertThat(explicitBounds, hasSize(3));
        assertThat(explicitBounds.get(0), closeTo(0.1, 0.001));
        assertThat(explicitBounds.get(1), closeTo(0.5, 0.001));
        assertThat(explicitBounds.get(2), closeTo(1.0, 0.001));

        assertThat(event.get("aggregationTemporality", String.class), equalTo("AGGREGATION_TEMPORALITY_CUMULATIVE"));

        @SuppressWarnings("unchecked")
        final Map<String, Object> attributes = event.get("attributes", Map.class);
        assertThat(attributes, not(hasKey("le")));
        assertThat(attributes, hasEntry("method", "GET"));
    }

    @Test
    void testParseHistogramWithNoCountAndSum() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("partial_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("1.0").build())
                .addSamples(Types.Sample.newBuilder().setValue(10.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("partial_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("+Inf").build())
                .addSamples(Types.Sample.newBuilder().setValue(20.0).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(1));
        final Event event = records.get(0).getData();
        assertThat(event.get("kind", String.class), equalTo(Metric.KIND.HISTOGRAM.toString()));
        assertThat(event.get("name", String.class), equalTo("partial"));
        assertThat(event.get("sum", Double.class), closeTo(0.0, 0.001));
        assertThat(event.get("count", Long.class), equalTo(0L));
    }

    @Test
    void testParseSummary() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("rpc_duration_seconds").build())
                .addLabels(Types.Label.newBuilder().setName("quantile").setValue("0.5").build())
                .addLabels(Types.Label.newBuilder().setName("service").setValue("api").build())
                .addSamples(Types.Sample.newBuilder().setValue(0.2).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("rpc_duration_seconds").build())
                .addLabels(Types.Label.newBuilder().setName("quantile").setValue("0.9").build())
                .addLabels(Types.Label.newBuilder().setName("service").setValue("api").build())
                .addSamples(Types.Sample.newBuilder().setValue(0.4).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("rpc_duration_seconds").build())
                .addLabels(Types.Label.newBuilder().setName("quantile").setValue("0.99").build())
                .addLabels(Types.Label.newBuilder().setName("service").setValue("api").build())
                .addSamples(Types.Sample.newBuilder().setValue(0.8).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("rpc_duration_seconds_count").build())
                .addLabels(Types.Label.newBuilder().setName("service").setValue("api").build())
                .addSamples(Types.Sample.newBuilder().setValue(1000.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("rpc_duration_seconds_sum").build())
                .addLabels(Types.Label.newBuilder().setName("service").setValue("api").build())
                .addSamples(Types.Sample.newBuilder().setValue(300.5).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(1));
        final Event event = records.get(0).getData();

        assertThat(event.get("kind", String.class), equalTo(Metric.KIND.SUMMARY.toString()));
        assertThat(event.get("name", String.class), equalTo("rpc_duration_seconds"));
        assertThat(event.get("sum", Double.class), closeTo(300.5, 0.001));
        assertThat(event.get("count", Long.class), equalTo(1000L));
        assertThat(event, instanceOf(Summary.class));

        final List<Map<String, Object>> quantiles = event.get("quantiles", List.class);
        assertThat(quantiles, hasSize(3));

        @SuppressWarnings("unchecked")
        final Map<String, Object> attributes = event.get("attributes", Map.class);
        assertThat(attributes, not(hasKey("quantile")));
        assertThat(attributes, hasEntry("service", "api"));
    }

    @Test
    void testServiceNameExtractedFromServiceDotName() throws Exception {
        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("test_metric").build())
                .addLabels(Types.Label.newBuilder().setName("service.name").setValue("my-service").build())
                .addLabels(Types.Label.newBuilder().setName("service_name").setValue("fallback-service").build())
                .addLabels(Types.Label.newBuilder().setName("job").setValue("job-service").build())
                .addSamples(Types.Sample.newBuilder().setValue(1.0).setTimestamp(1706869800000L).build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().addTimeseries(timeSeries).build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records.size(), equalTo(1));
        assertThat(records.get(0).getData().get("serviceName", String.class), equalTo("my-service"));
    }

    @Test
    void testServiceNameFallsBackToServiceUnderscore() throws Exception {
        final Types.TimeSeries timeSeries = Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("test_metric").build())
                .addLabels(Types.Label.newBuilder().setName("service_name").setValue("fallback-service").build())
                .addLabels(Types.Label.newBuilder().setName("job").setValue("job-service").build())
                .addSamples(Types.Sample.newBuilder().setValue(1.0).setTimestamp(1706869800000L).build())
                .build();

        final byte[] compressed = Snappy.compress(
                Remote.WriteRequest.newBuilder().addTimeseries(timeSeries).build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records.size(), equalTo(1));
        assertThat(records.get(0).getData().get("serviceName", String.class), equalTo("fallback-service"));
    }

    @Test
    void testExtractServiceNamePriorityOrder() {
        final Map<String, Object> attrs = new HashMap<>();
        attrs.put("service.name", "svc1");
        attrs.put("service_name", "svc2");
        attrs.put("job", "svc3");
        assertThat(RemoteWriteProtobufParser.extractServiceName(attrs), equalTo("svc1"));

        attrs.remove("service.name");
        assertThat(RemoteWriteProtobufParser.extractServiceName(attrs), equalTo("svc2"));

        attrs.remove("service_name");
        assertThat(RemoteWriteProtobufParser.extractServiceName(attrs), equalTo("svc3"));

        attrs.remove("job");
        assertThat(RemoteWriteProtobufParser.extractServiceName(attrs), equalTo(""));
    }

    @Test
    void testStripCounterSuffix() {
        assertThat(RemoteWriteProtobufParser.stripCounterSuffix("http_requests_total"), equalTo("http_requests"));
        assertThat(RemoteWriteProtobufParser.stripCounterSuffix("cpu_temperature"), equalTo("cpu_temperature"));
        assertThat(RemoteWriteProtobufParser.stripCounterSuffix("http_requests_created"), equalTo("http_requests"));
    }

    @Test
    void testParseMixedMetricTypes() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("cpu_temperature").build())
                .addSamples(Types.Sample.newBuilder().setValue(72.5).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("http_requests_total").build())
                .addSamples(Types.Sample.newBuilder().setValue(100.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("request_duration_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("0.5").build())
                .addSamples(Types.Sample.newBuilder().setValue(10.0).setTimestamp(1706869800000L).build())
                .build());
        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("request_duration_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("+Inf").build())
                .addSamples(Types.Sample.newBuilder().setValue(15.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("response_latency").build())
                .addLabels(Types.Label.newBuilder().setName("quantile").setValue("0.5").build())
                .addSamples(Types.Sample.newBuilder().setValue(0.2).setTimestamp(1706869800000L).build())
                .build());
        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("response_latency").build())
                .addLabels(Types.Label.newBuilder().setName("quantile").setValue("0.99").build())
                .addSamples(Types.Sample.newBuilder().setValue(0.8).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(4));

        long histogramCount = records.stream()
                .filter(r -> Metric.KIND.HISTOGRAM.toString().equals(r.getData().get("kind", String.class)))
                .count();
        long summaryCount = records.stream()
                .filter(r -> Metric.KIND.SUMMARY.toString().equals(r.getData().get("kind", String.class)))
                .count();
        long gaugeCount = records.stream()
                .filter(r -> Metric.KIND.GAUGE.toString().equals(r.getData().get("kind", String.class)))
                .count();
        long sumCount = records.stream()
                .filter(r -> Metric.KIND.SUM.toString().equals(r.getData().get("kind", String.class)))
                .count();

        assertThat(histogramCount, equalTo(1L));
        assertThat(summaryCount, equalTo(1L));
        assertThat(gaugeCount, equalTo(1L));
        assertThat(sumCount, equalTo(1L));
    }

    @Test
    void testParseHistogramWithFlattenLabels() throws Exception {
        lenient().when(config.isFlattenLabels()).thenReturn(true);
        final RemoteWriteProtobufParser flattenParser = new RemoteWriteProtobufParser(config);

        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("test_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("1.0").build())
                .addLabels(Types.Label.newBuilder().setName("env").setValue("prod").build())
                .addSamples(Types.Sample.newBuilder().setValue(5.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("test_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("+Inf").build())
                .addLabels(Types.Label.newBuilder().setName("env").setValue("prod").build())
                .addSamples(Types.Sample.newBuilder().setValue(10.0).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = flattenParser.parse(compressed);

        assertThat(records, hasSize(1));
        assertThat(records.get(0).getData().get("kind", String.class), equalTo(Metric.KIND.HISTOGRAM.toString()));
    }

    @Test
    void testHistogramWithMultipleSamplesProducesMultipleEvents() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();
        final long ts1 = 1706869800000L;
        final long ts2 = 1706869860000L;

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("dur_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("1.0").build())
                .addSamples(Types.Sample.newBuilder().setValue(5.0).setTimestamp(ts1).build())
                .addSamples(Types.Sample.newBuilder().setValue(8.0).setTimestamp(ts2).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("dur_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("+Inf").build())
                .addSamples(Types.Sample.newBuilder().setValue(10.0).setTimestamp(ts1).build())
                .addSamples(Types.Sample.newBuilder().setValue(15.0).setTimestamp(ts2).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(2));

        final Event event1 = records.get(0).getData();
        assertThat(event1.get("kind", String.class), equalTo(Metric.KIND.HISTOGRAM.toString()));
        assertThat(event1.get("time", String.class), equalTo(Instant.ofEpochMilli(ts1).toString()));
        final List<Long> buckets1 = event1.get("bucketCountsList", List.class);
        assertThat(buckets1, hasSize(2));
        assertThat(buckets1.get(0), equalTo(5L));
        assertThat(buckets1.get(1), equalTo(5L));

        final Event event2 = records.get(1).getData();
        assertThat(event2.get("time", String.class), equalTo(Instant.ofEpochMilli(ts2).toString()));
        final List<Long> buckets2 = event2.get("bucketCountsList", List.class);
        assertThat(buckets2.get(0), equalTo(8L));
        assertThat(buckets2.get(1), equalTo(7L));
    }

    @Test
    void testSummaryWithMultipleSamplesProducesMultipleEvents() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();
        final long ts1 = 1706869800000L;
        final long ts2 = 1706869860000L;

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("latency").build())
                .addLabels(Types.Label.newBuilder().setName("quantile").setValue("0.5").build())
                .addSamples(Types.Sample.newBuilder().setValue(0.1).setTimestamp(ts1).build())
                .addSamples(Types.Sample.newBuilder().setValue(0.2).setTimestamp(ts2).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("latency").build())
                .addLabels(Types.Label.newBuilder().setName("quantile").setValue("0.99").build())
                .addSamples(Types.Sample.newBuilder().setValue(0.5).setTimestamp(ts1).build())
                .addSamples(Types.Sample.newBuilder().setValue(0.8).setTimestamp(ts2).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(2));
        assertThat(records.get(0).getData().get("kind", String.class), equalTo(Metric.KIND.SUMMARY.toString()));
        assertThat(records.get(0).getData().get("time", String.class), equalTo(Instant.ofEpochMilli(ts1).toString()));
        assertThat(records.get(1).getData().get("time", String.class), equalTo(Instant.ofEpochMilli(ts2).toString()));
    }

    @Test
    void testHistogramGroupedByLabelSet() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("dur_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("1.0").build())
                .addLabels(Types.Label.newBuilder().setName("method").setValue("GET").build())
                .addSamples(Types.Sample.newBuilder().setValue(10.0).setTimestamp(1706869800000L).build())
                .build());
        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("dur_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("+Inf").build())
                .addLabels(Types.Label.newBuilder().setName("method").setValue("GET").build())
                .addSamples(Types.Sample.newBuilder().setValue(20.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("dur_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("1.0").build())
                .addLabels(Types.Label.newBuilder().setName("method").setValue("POST").build())
                .addSamples(Types.Sample.newBuilder().setValue(3.0).setTimestamp(1706869800000L).build())
                .build());
        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("dur_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("+Inf").build())
                .addLabels(Types.Label.newBuilder().setName("method").setValue("POST").build())
                .addSamples(Types.Sample.newBuilder().setValue(5.0).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(2));

        final Event getEvent = records.stream()
                .filter(r -> {
                    Map<String, Object> attrs = r.getData().get("attributes", Map.class);
                    return "GET".equals(attrs.get("method"));
                })
                .findFirst().get().getData();

        final Event postEvent = records.stream()
                .filter(r -> {
                    Map<String, Object> attrs = r.getData().get("attributes", Map.class);
                    return "POST".equals(attrs.get("method"));
                })
                .findFirst().get().getData();

        final List<Long> getBuckets = getEvent.get("bucketCountsList", List.class);
        assertThat(getBuckets.get(0), equalTo(10L));
        assertThat(getBuckets.get(1), equalTo(10L));

        final List<Long> postBuckets = postEvent.get("bucketCountsList", List.class);
        assertThat(postBuckets.get(0), equalTo(3L));
        assertThat(postBuckets.get(1), equalTo(2L));
    }

    @Test
    void testParseLeValueValidAndInvalid() {
        assertThat(RemoteWriteProtobufParser.parseLeValue("0.5"), closeTo(0.5, 0.001));
        assertThat(RemoteWriteProtobufParser.parseLeValue("+Inf"), equalTo(Double.POSITIVE_INFINITY));
        assertThat(RemoteWriteProtobufParser.parseLeValue(null), nullValue());
        assertThat(RemoteWriteProtobufParser.parseLeValue("not_a_number"), nullValue());
    }

    @Test
    void testParseQuantileValueValidAndInvalid() {
        assertThat(RemoteWriteProtobufParser.parseQuantileValue("0.99"), closeTo(0.99, 0.001));
        assertThat(RemoteWriteProtobufParser.parseQuantileValue(null), nullValue());
        assertThat(RemoteWriteProtobufParser.parseQuantileValue("not_a_number"), nullValue());
    }

    @Test
    void testHistogramWithUnparseableLeSkipsBucket() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("test_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("1.0").build())
                .addSamples(Types.Sample.newBuilder().setValue(5.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("test_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("bad_value").build())
                .addSamples(Types.Sample.newBuilder().setValue(10.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("test_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("+Inf").build())
                .addSamples(Types.Sample.newBuilder().setValue(15.0).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(1));
        final List<Long> bucketCounts = records.get(0).getData().get("bucketCountsList", List.class);
        assertThat(bucketCounts, hasSize(2));
    }

    @Test
    void testHistogramNegativeCumulativeDiffClampedToZero() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("bad_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("0.5").build())
                .addSamples(Types.Sample.newBuilder().setValue(20.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("bad_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("1.0").build())
                .addSamples(Types.Sample.newBuilder().setValue(10.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("bad_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("+Inf").build())
                .addSamples(Types.Sample.newBuilder().setValue(25.0).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(1));
        final List<Long> bucketCounts = records.get(0).getData().get("bucketCountsList", List.class);
        assertThat(bucketCounts, hasSize(3));
        assertThat(bucketCounts.get(0), equalTo(20L));
        assertThat(bucketCounts.get(1), equalTo(0L));
        assertThat(bucketCounts.get(2), greaterThanOrEqualTo(0L));
    }

    @Test
    void testHistogramCountSumTimestampMismatchDefaultsToZero() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();
        final long bucketTs = 1706869800000L;
        final long countSumTs = 1706869860000L;

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("ts_mismatch_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("1.0").build())
                .addSamples(Types.Sample.newBuilder().setValue(5.0).setTimestamp(bucketTs).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("ts_mismatch_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("+Inf").build())
                .addSamples(Types.Sample.newBuilder().setValue(10.0).setTimestamp(bucketTs).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("ts_mismatch_count").build())
                .addSamples(Types.Sample.newBuilder().setValue(999.0).setTimestamp(countSumTs).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("ts_mismatch_sum").build())
                .addSamples(Types.Sample.newBuilder().setValue(123.4).setTimestamp(countSumTs).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(1));
        final Event event = records.get(0).getData();
        assertThat(event.get("kind", String.class), equalTo(Metric.KIND.HISTOGRAM.toString()));
        assertThat(event.get("time", String.class), equalTo(Instant.ofEpochMilli(bucketTs).toString()));
        assertThat(event.get("count", Long.class), equalTo(0L));
        assertThat(event.get("sum", Double.class), closeTo(0.0, 0.001));
    }

    @Test
    void testParseDecompressedWithInvalidProtobuf() {
        final byte[] invalidProtobuf = "not valid protobuf data".getBytes();

        assertThrows(PrometheusParseException.class, () -> parser.parseDecompressed(invalidProtobuf));
    }

    @Test
    void testParseStandaloneSumMetricNotMatchingHistogramOrSummary() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("standalone_metric_sum").build())
                .addLabels(Types.Label.newBuilder().setName("host").setValue("server-01").build())
                .addSamples(Types.Sample.newBuilder().setValue(42.0).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(1));
        final Event event = records.get(0).getData();
        assertThat(event.get("kind", String.class), equalTo(Metric.KIND.GAUGE.toString()));
        assertThat(event.get("name", String.class), equalTo("standalone_metric_sum"));
    }

    @Test
    void testParseBucketSuffixWithoutLeLabel() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("fake_metric_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("host").setValue("server-01").build())
                .addSamples(Types.Sample.newBuilder().setValue(5.0).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(1));
        final Event event = records.get(0).getData();
        assertThat(event.get("kind", String.class), equalTo(Metric.KIND.GAUGE.toString()));
        assertThat(event.get("name", String.class), equalTo("fake_metric_bucket"));
    }

    @Test
    void testParseHistogramBucketsWithNoSamples() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("empty_hist_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("1.0").build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(0));
    }

    @Test
    void testParseSummaryQuantilesWithNoSamples() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("empty_summary").build())
                .addLabels(Types.Label.newBuilder().setName("quantile").setValue("0.5").build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(0));
    }

    @Test
    void testParseHistogramWithUnparseableLeValues() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("bad_le_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("not_a_number").build())
                .addSamples(Types.Sample.newBuilder().setValue(5.0).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(0));
    }

    @Test
    void testParseSummaryWithUnparseableQuantileValues() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("bad_quantile").build())
                .addLabels(Types.Label.newBuilder().setName("quantile").setValue("not_a_number").build())
                .addSamples(Types.Sample.newBuilder().setValue(5.0).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(0));
    }

    @Test
    void testParseHistogramCountBeforeBuckets() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("order_test_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("1.0").build())
                .addSamples(Types.Sample.newBuilder().setValue(5.0).setTimestamp(1706869800000L).build())
                .build());
        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("order_test_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("+Inf").build())
                .addSamples(Types.Sample.newBuilder().setValue(10.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("order_test_count").build())
                .addLabels(Types.Label.newBuilder().setName("env").setValue("prod").build())
                .addSamples(Types.Sample.newBuilder().setValue(10.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("order_test_sum").build())
                .addLabels(Types.Label.newBuilder().setName("env").setValue("prod").build())
                .addSamples(Types.Sample.newBuilder().setValue(50.0).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records.isEmpty(), equalTo(false));
    }

    @Test
    void testParseSummaryCountBeforeQuantiles() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("summary_order_test").build())
                .addLabels(Types.Label.newBuilder().setName("quantile").setValue("0.5").build())
                .addSamples(Types.Sample.newBuilder().setValue(0.2).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("summary_order_test_count").build())
                .addLabels(Types.Label.newBuilder().setName("env").setValue("prod").build())
                .addSamples(Types.Sample.newBuilder().setValue(100.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("summary_order_test_sum").build())
                .addLabels(Types.Label.newBuilder().setName("env").setValue("prod").build())
                .addSamples(Types.Sample.newBuilder().setValue(50.0).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records.isEmpty(), equalTo(false));
    }

    @Test
    void testParseHistogramWithMultipleTimestampsAndMismatchedBucketTimestamps() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("ts_test_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("not_valid").build())
                .addSamples(Types.Sample.newBuilder().setValue(5.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("ts_test_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("1.0").build())
                .addSamples(Types.Sample.newBuilder().setValue(10.0).setTimestamp(1706869860000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(1));
    }

    @Test
    void testParseHistogramGroupWithEmptyBucketsFromMismatchedLabels() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("label_mismatch_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("1.0").build())
                .addLabels(Types.Label.newBuilder().setName("env").setValue("prod").build())
                .addSamples(Types.Sample.newBuilder().setValue(5.0).setTimestamp(1706869800000L).build())
                .build());
        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("label_mismatch_bucket").build())
                .addLabels(Types.Label.newBuilder().setName("le").setValue("+Inf").build())
                .addLabels(Types.Label.newBuilder().setName("env").setValue("prod").build())
                .addSamples(Types.Sample.newBuilder().setValue(10.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("label_mismatch_count").build())
                .addLabels(Types.Label.newBuilder().setName("env").setValue("staging").build())
                .addSamples(Types.Sample.newBuilder().setValue(99.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("label_mismatch_sum").build())
                .addLabels(Types.Label.newBuilder().setName("env").setValue("staging").build())
                .addSamples(Types.Sample.newBuilder().setValue(500.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("label_mismatch_sum").build())
                .addLabels(Types.Label.newBuilder().setName("env").setValue("dev").build())
                .addSamples(Types.Sample.newBuilder().setValue(200.0).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(1));
    }

    @Test
    void testParseSummaryGroupWithEmptyQuantilesFromMismatchedLabels() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("latency_mismatch").build())
                .addLabels(Types.Label.newBuilder().setName("quantile").setValue("0.5").build())
                .addLabels(Types.Label.newBuilder().setName("env").setValue("prod").build())
                .addSamples(Types.Sample.newBuilder().setValue(0.2).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("latency_mismatch_count").build())
                .addLabels(Types.Label.newBuilder().setName("env").setValue("staging").build())
                .addSamples(Types.Sample.newBuilder().setValue(100.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("latency_mismatch_sum").build())
                .addLabels(Types.Label.newBuilder().setName("env").setValue("staging").build())
                .addSamples(Types.Sample.newBuilder().setValue(50.0).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("latency_mismatch_sum").build())
                .addLabels(Types.Label.newBuilder().setName("env").setValue("dev").build())
                .addSamples(Types.Sample.newBuilder().setValue(25.0).setTimestamp(1706869800000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(1));
    }

    @Test
    void testParseSummaryWithMismatchedTimestampsBetweenQuantiles() throws Exception {
        final Remote.WriteRequest.Builder requestBuilder = Remote.WriteRequest.newBuilder();

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("ts_mismatch_summary").build())
                .addLabels(Types.Label.newBuilder().setName("quantile").setValue("0.5").build())
                .addSamples(Types.Sample.newBuilder().setValue(0.2).setTimestamp(1706869800000L).build())
                .build());

        requestBuilder.addTimeseries(Types.TimeSeries.newBuilder()
                .addLabels(Types.Label.newBuilder().setName("__name__").setValue("ts_mismatch_summary").build())
                .addLabels(Types.Label.newBuilder().setName("quantile").setValue("0.99").build())
                .addSamples(Types.Sample.newBuilder().setValue(0.8).setTimestamp(1706869860000L).build())
                .build());

        final byte[] compressed = Snappy.compress(requestBuilder.build().toByteArray());
        final List<Record<Event>> records = parser.parse(compressed);

        assertThat(records, hasSize(2));
    }
}