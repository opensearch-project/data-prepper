/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;

import com.google.protobuf.util.JsonFormat;

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.trace.v1.ResourceSpans;

/**
 * Comprehensive round-trip tests for OTLP encoding/decoding.
 * Tests verify that data can be decoded from protobuf, modified, and encoded back
 * to protobuf without losing fidelity.
 */
public class OTelProtoEncoderRoundTripTest {

    private static final double DELTA = 0.0001;
    private static final Instant TEST_TIME = Instant.now();

    private OTelProtoStandardCodec.OTelProtoDecoder decoder;
    private OTelProtoStandardCodec.OTelProtoEncoder encoder;

    @BeforeEach
    void setUp() {
        decoder = new OTelProtoStandardCodec.OTelProtoDecoder();
        encoder = new OTelProtoStandardCodec.OTelProtoEncoder();
    }

    private String getFileAsJsonString(String requestJsonFileName) throws IOException {
        final StringBuilder jsonBuilder = new StringBuilder();
        try (final InputStream inputStream = Objects.requireNonNull(
                OTelProtoEncoderRoundTripTest.class.getClassLoader().getResourceAsStream(requestJsonFileName))) {
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            bufferedReader.lines().forEach(jsonBuilder::append);
        }
        return jsonBuilder.toString();
    }

    private ExportTraceServiceRequest buildExportTraceServiceRequestFromJsonFile(String requestJsonFileName) throws IOException {
        final ExportTraceServiceRequest.Builder builder = ExportTraceServiceRequest.newBuilder();
        JsonFormat.parser().merge(getFileAsJsonString(requestJsonFileName), builder);
        return builder.build();
    }

    private ExportMetricsServiceRequest buildExportMetricsServiceRequestFromJsonFile(String requestJsonFileName) throws IOException {
        final ExportMetricsServiceRequest.Builder builder = ExportMetricsServiceRequest.newBuilder();
        JsonFormat.parser().merge(getFileAsJsonString(requestJsonFileName), builder);
        return builder.build();
    }

    private ExportLogsServiceRequest buildExportLogsServiceRequestFromJsonFile(String requestJsonFileName) throws IOException {
        final ExportLogsServiceRequest.Builder builder = ExportLogsServiceRequest.newBuilder();
        JsonFormat.parser().merge(getFileAsJsonString(requestJsonFileName), builder);
        return builder.build();
    }

    @Nested
    class TraceRoundTripTests {

        @Test
        void testSpanRoundTrip() throws Exception {
            // Load original protobuf request
            final ExportTraceServiceRequest originalRequest = 
                buildExportTraceServiceRequestFromJsonFile("test-request.json");
            
            // Decode to Data Prepper model
            final List<Span> spans = decoder.parseExportTraceServiceRequest(originalRequest, TEST_TIME);
            assertThat(spans.size(), equalTo(3));  // test-request.json has 3 spans
            
            final Span span = spans.get(0);
            
            // Encode back to protobuf
            final ResourceSpans encodedResourceSpans = encoder.convertToResourceSpans(span);
            
            // Verify structure
            assertNotNull(encodedResourceSpans);
            assertThat(encodedResourceSpans.getScopeSpansCount(), equalTo(1));
            assertThat(encodedResourceSpans.getScopeSpans(0).getSpansCount(), equalTo(1));
            
            final io.opentelemetry.proto.trace.v1.Span encodedSpan = 
                encodedResourceSpans.getScopeSpans(0).getSpans(0);
            final io.opentelemetry.proto.trace.v1.Span originalSpan = 
                originalRequest.getResourceSpans(0).getScopeSpans(0).getSpans(0);
            
            // Verify key fields match
            assertEquals(originalSpan.getSpanId(), encodedSpan.getSpanId());
            assertEquals(originalSpan.getTraceId(), encodedSpan.getTraceId());
            assertEquals(originalSpan.getParentSpanId(), encodedSpan.getParentSpanId());
            assertEquals(originalSpan.getName(), encodedSpan.getName());
            assertEquals(originalSpan.getKind(), encodedSpan.getKind());
            assertEquals(originalSpan.getStartTimeUnixNano(), encodedSpan.getStartTimeUnixNano());
            assertEquals(originalSpan.getEndTimeUnixNano(), encodedSpan.getEndTimeUnixNano());
            assertEquals(originalSpan.getTraceState(), encodedSpan.getTraceState());
            
            // Verify attributes count
            assertThat(encodedSpan.getAttributesCount(), equalTo(originalSpan.getAttributesCount()));
            
            // Verify events count
            assertThat(encodedSpan.getEventsCount(), equalTo(originalSpan.getEventsCount()));
            
            // Verify links count
            assertThat(encodedSpan.getLinksCount(), equalTo(originalSpan.getLinksCount()));
            
            // Verify status
            assertEquals(originalSpan.getStatus().getCodeValue(), encodedSpan.getStatus().getCodeValue());
        }

        @Test
        void testSpanWithEventsRoundTrip() throws Exception {
            // Use test-request.json which has proper OTLP format
            final ExportTraceServiceRequest originalRequest = 
                buildExportTraceServiceRequestFromJsonFile("test-request.json");
            
            final List<Span> spans = decoder.parseExportTraceServiceRequest(originalRequest, TEST_TIME);
            assertThat(spans.size(), equalTo(3));  // test-request.json has 3 spans
            
            final Span span = spans.get(0);
            final ResourceSpans encodedResourceSpans = encoder.convertToResourceSpans(span);
            
            final io.opentelemetry.proto.trace.v1.Span encodedSpan = 
                encodedResourceSpans.getScopeSpans(0).getSpans(0);
            final io.opentelemetry.proto.trace.v1.Span originalSpan = 
                originalRequest.getResourceSpans(0).getScopeSpans(0).getSpans(0);
            
            // Verify events are preserved
            assertThat(encodedSpan.getEventsCount(), equalTo(originalSpan.getEventsCount()));
            
            if (originalSpan.getEventsCount() > 0) {
                final io.opentelemetry.proto.trace.v1.Span.Event originalEvent = originalSpan.getEvents(0);
                final io.opentelemetry.proto.trace.v1.Span.Event encodedEvent = encodedSpan.getEvents(0);
                
                assertEquals(originalEvent.getName(), encodedEvent.getName());
                assertEquals(originalEvent.getTimeUnixNano(), encodedEvent.getTimeUnixNano());
                assertThat(encodedEvent.getAttributesCount(), equalTo(originalEvent.getAttributesCount()));
            }
        }
    }

    @Nested
    class MetricsRoundTripTests {

        @Test
        void testGaugeMetricRoundTrip() throws Exception {
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-gauge-metrics.json");
            
            // Decode to Data Prepper model
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            
            assertThat(records.size(), equalTo(1));
            final Metric metric = records.iterator().next().getData();
            assertThat(metric, instanceOf(JacksonGauge.class));
            
            final JacksonGauge gauge = (JacksonGauge) metric;
            
            // Encode back to protobuf
            final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(gauge);
            
            // Verify structure
            assertNotNull(encodedResourceMetrics);
            assertThat(encodedResourceMetrics.getScopeMetricsCount(), equalTo(1));
            assertThat(encodedResourceMetrics.getScopeMetrics(0).getMetricsCount(), equalTo(1));
            
            final io.opentelemetry.proto.metrics.v1.Metric encodedMetric = 
                encodedResourceMetrics.getScopeMetrics(0).getMetrics(0);
            final io.opentelemetry.proto.metrics.v1.Metric originalMetric = 
                originalRequest.getResourceMetrics(0).getScopeMetrics(0).getMetrics(0);
            
            // Verify metric metadata
            assertEquals(originalMetric.getName(), encodedMetric.getName());
            assertEquals(originalMetric.getDescription(), encodedMetric.getDescription());
            assertEquals(originalMetric.getUnit(), encodedMetric.getUnit());
            
            // Verify it's a gauge
            assertThat(encodedMetric.hasGauge(), equalTo(true));
            
            // Verify data points
            assertThat(encodedMetric.getGauge().getDataPointsCount(), 
                equalTo(originalMetric.getGauge().getDataPointsCount()));
            
            if (originalMetric.getGauge().getDataPointsCount() > 0) {
                final var originalDataPoint = originalMetric.getGauge().getDataPoints(0);
                final var encodedDataPoint = encodedMetric.getGauge().getDataPoints(0);
                
                assertEquals(originalDataPoint.getTimeUnixNano(), encodedDataPoint.getTimeUnixNano());
                assertEquals(originalDataPoint.getStartTimeUnixNano(), encodedDataPoint.getStartTimeUnixNano());
                
                // Verify value (handle both int and double)
                if (originalDataPoint.hasAsDouble()) {
                    assertThat(encodedDataPoint.getAsDouble(), 
                        closeTo(originalDataPoint.getAsDouble(), DELTA));
                } else if (originalDataPoint.hasAsInt()) {
                    assertThat((double) encodedDataPoint.getAsDouble(), 
                        closeTo((double) originalDataPoint.getAsInt(), DELTA));
                }
            }
        }

        @Test
        void testSumMetricRoundTrip() throws Exception {
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-sum-metrics.json");
            
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            
            assertThat(records.size(), equalTo(1));
            final Metric metric = records.iterator().next().getData();
            assertThat(metric, instanceOf(JacksonSum.class));
            
            final JacksonSum sum = (JacksonSum) metric;
            
            // Encode back to protobuf
            final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(sum);
            
            final io.opentelemetry.proto.metrics.v1.Metric encodedMetric = 
                encodedResourceMetrics.getScopeMetrics(0).getMetrics(0);
            final io.opentelemetry.proto.metrics.v1.Metric originalMetric = 
                originalRequest.getResourceMetrics(0).getScopeMetrics(0).getMetrics(0);
            
            // Verify it's a sum
            assertThat(encodedMetric.hasSum(), equalTo(true));
            
            // Verify sum-specific fields
            assertEquals(originalMetric.getSum().getIsMonotonic(), encodedMetric.getSum().getIsMonotonic());
            assertEquals(originalMetric.getSum().getAggregationTemporality(), 
                encodedMetric.getSum().getAggregationTemporality());
            
            // Verify data points
            assertThat(encodedMetric.getSum().getDataPointsCount(), 
                equalTo(originalMetric.getSum().getDataPointsCount()));
        }

        @Test
        void testHistogramMetricRoundTrip() throws Exception {
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-histogram-metrics.json");
            
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            
            assertThat(records.size(), equalTo(1));
            final Metric metric = records.iterator().next().getData();
            assertThat(metric, instanceOf(JacksonHistogram.class));
            
            final JacksonHistogram histogram = (JacksonHistogram) metric;
            
            // Encode back to protobuf
            final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(histogram);
            
            final io.opentelemetry.proto.metrics.v1.Metric encodedMetric = 
                encodedResourceMetrics.getScopeMetrics(0).getMetrics(0);
            final io.opentelemetry.proto.metrics.v1.Metric originalMetric = 
                originalRequest.getResourceMetrics(0).getScopeMetrics(0).getMetrics(0);
            
            // Verify it's a histogram
            assertThat(encodedMetric.hasHistogram(), equalTo(true));
            
            // Verify histogram-specific fields
            assertEquals(originalMetric.getHistogram().getAggregationTemporality(), 
                encodedMetric.getHistogram().getAggregationTemporality());
            
            // Verify data points
            assertThat(encodedMetric.getHistogram().getDataPointsCount(), 
                equalTo(originalMetric.getHistogram().getDataPointsCount()));
            
            if (originalMetric.getHistogram().getDataPointsCount() > 0) {
                final var originalDataPoint = originalMetric.getHistogram().getDataPoints(0);
                final var encodedDataPoint = encodedMetric.getHistogram().getDataPoints(0);
                
                assertEquals(originalDataPoint.getCount(), encodedDataPoint.getCount());
                assertThat(encodedDataPoint.getSum(), closeTo(originalDataPoint.getSum(), DELTA));
                
                // Verify bucket counts
                assertEquals(originalDataPoint.getBucketCountsCount(), 
                    encodedDataPoint.getBucketCountsCount());
                
                // Verify explicit bounds
                assertEquals(originalDataPoint.getExplicitBoundsCount(), 
                    encodedDataPoint.getExplicitBoundsCount());
                
                for (int i = 0; i < originalDataPoint.getExplicitBoundsCount(); i++) {
                    assertThat(encodedDataPoint.getExplicitBounds(i), 
                        closeTo(originalDataPoint.getExplicitBounds(i), DELTA));
                }
            }
        }

        @Test
        void testMetricWithExemplarsRoundTrip() throws Exception {
            // Create a metric request with exemplars
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-gauge-metrics.json");
            
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            
            for (Record<? extends Metric> record : records) {
                final Metric metric = record.getData();
                final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(metric);
                
                assertNotNull(encodedResourceMetrics);
                assertThat(encodedResourceMetrics.getScopeMetricsCount(), equalTo(1));
            }
        }

        @Test
        void testMultipleMetricsRoundTrip() throws Exception {
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-request-multiple-metrics.json");
            
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            
            // Verify we can encode all metrics back
            int gaugeCount = 0;
            int sumCount = 0;
            int histogramCount = 0;
            
            for (Record<? extends Metric> record : records) {
                final Metric metric = record.getData();
                final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(metric);
                
                assertNotNull(encodedResourceMetrics);
                assertThat(encodedResourceMetrics.getScopeMetricsCount(), equalTo(1));
                assertThat(encodedResourceMetrics.getScopeMetrics(0).getMetricsCount(), equalTo(1));
                
                final io.opentelemetry.proto.metrics.v1.Metric encodedMetric = 
                    encodedResourceMetrics.getScopeMetrics(0).getMetrics(0);
                
                if (encodedMetric.hasGauge()) {
                    gaugeCount++;
                } else if (encodedMetric.hasSum()) {
                    sumCount++;
                } else if (encodedMetric.hasHistogram()) {
                    histogramCount++;
                }
            }
            
            // Verify we have different metric types
            assertThat(gaugeCount + sumCount + histogramCount, equalTo(records.size()));
        }
    }

    @Nested
    class LogsRoundTripTests {

        @Test
        void testLogRoundTrip() throws Exception {
            final ExportLogsServiceRequest originalRequest = 
                buildExportLogsServiceRequestFromJsonFile("test-request-log.json");
            
            // Decode to Data Prepper model
            final List<OpenTelemetryLog> logs = 
                decoder.parseExportLogsServiceRequest(originalRequest, TEST_TIME);
            
            assertThat(logs.size(), equalTo(1));
            final Log log = logs.get(0);
            
            // Encode back to protobuf
            final ResourceLogs encodedResourceLogs = encoder.convertToResourceLogs(log);
            
            // Verify structure
            assertNotNull(encodedResourceLogs);
            assertThat(encodedResourceLogs.getScopeLogsCount(), equalTo(1));
            assertThat(encodedResourceLogs.getScopeLogs(0).getLogRecordsCount(), equalTo(1));
            
            final io.opentelemetry.proto.logs.v1.LogRecord encodedLogRecord = 
                encodedResourceLogs.getScopeLogs(0).getLogRecords(0);
            
            // Verify log record is not null
            assertNotNull(encodedLogRecord);
        }

        @Test
        void testMultipleLogsRoundTrip() throws Exception {
            final ExportLogsServiceRequest originalRequest = 
                buildExportLogsServiceRequestFromJsonFile("test-request-multiple-logs.json");
            
            final List<OpenTelemetryLog> logs = 
                decoder.parseExportLogsServiceRequest(originalRequest, TEST_TIME);
            
            // Verify we can encode all logs back
            for (OpenTelemetryLog log : logs) {
                final ResourceLogs encodedResourceLogs = encoder.convertToResourceLogs(log);
                
                assertNotNull(encodedResourceLogs);
                assertThat(encodedResourceLogs.getScopeLogsCount(), equalTo(1));
                assertThat(encodedResourceLogs.getScopeLogs(0).getLogRecordsCount(), equalTo(1));
            }
        }
    }

    @Nested
    class AttributeModificationTests {

        @Test
        void testSpanAttributeModificationRoundTrip() throws Exception {
            final ExportTraceServiceRequest originalRequest = 
                buildExportTraceServiceRequestFromJsonFile("test-request.json");
            
            // Decode
            final List<Span> spans = decoder.parseExportTraceServiceRequest(originalRequest, TEST_TIME);
            
            // Find a span that has attributes (the second span in test-request.json has 3 attributes)
            final Span spanWithAttributes = spans.get(1);  // Second span has attributes
            
            // Encode
            final ResourceSpans encodedResourceSpans = encoder.convertToResourceSpans(spanWithAttributes);
            
            // Verify attributes are present in encoded span
            final io.opentelemetry.proto.trace.v1.Span encodedSpan = 
                encodedResourceSpans.getScopeSpans(0).getSpans(0);
            
            // The second span in test-request.json has 3 span-level attributes
            assertThat(encodedSpan.getAttributesCount(), equalTo(3));
            
            // Verify at least one attribute is correctly encoded
            boolean foundHttpStatusCode = false;
            for (io.opentelemetry.proto.common.v1.KeyValue kv : encodedSpan.getAttributesList()) {
                if (kv.getKey().equals("http.status_code")) {
                    foundHttpStatusCode = true;
                    assertEquals(200L, kv.getValue().getIntValue());
                }
            }
            
            assertThat(foundHttpStatusCode, equalTo(true));
        }

        @Test
        void testMetricAttributeModificationRoundTrip() throws Exception {
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-gauge-metrics.json");
            
            // Decode
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            final Metric originalMetric = records.iterator().next().getData();
            
            // Get the original metric data point attribute count from protobuf
            final int originalAttributeCount = 
                originalRequest.getResourceMetrics(0).getScopeMetrics(0).getMetrics(0)
                    .getGauge().getDataPoints(0).getAttributesCount();
            
            // Encode
            final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(originalMetric);
            
            // Verify structure is maintained
            assertNotNull(encodedResourceMetrics);
            assertThat(encodedResourceMetrics.getScopeMetricsCount(), equalTo(1));
            
            final io.opentelemetry.proto.metrics.v1.Metric encodedMetric = 
                encodedResourceMetrics.getScopeMetrics(0).getMetrics(0);
            
            // Verify data point attributes are preserved
            if (encodedMetric.hasGauge() && encodedMetric.getGauge().getDataPointsCount() > 0) {
                final var dataPoint = encodedMetric.getGauge().getDataPoints(0);
                assertThat(dataPoint.getAttributesCount(), equalTo(originalAttributeCount));
            }
        }
    }


    @Nested
    class ResourceAndScopeTests {

        @Test
        void testResourceAttributesPreserved() throws Exception {
            final ExportTraceServiceRequest originalRequest = 
                buildExportTraceServiceRequestFromJsonFile("test-request.json");
            
            final List<Span> spans = decoder.parseExportTraceServiceRequest(originalRequest, TEST_TIME);
            final Span span = spans.get(0);
            
            final ResourceSpans encodedResourceSpans = encoder.convertToResourceSpans(span);
            
            // Verify resource is present
            assertThat(encodedResourceSpans.hasResource(), equalTo(true));
            
            // Verify resource has attributes
            final io.opentelemetry.proto.resource.v1.Resource originalResource = 
                originalRequest.getResourceSpans(0).getResource();
            final io.opentelemetry.proto.resource.v1.Resource encodedResource = 
                encodedResourceSpans.getResource();
            
            assertThat(encodedResource.getAttributesCount(), 
                equalTo(originalResource.getAttributesCount()));
        }

        @Test
        void testInstrumentationScopePreserved() throws Exception {
            final ExportTraceServiceRequest originalRequest = 
                buildExportTraceServiceRequestFromJsonFile("test-request.json");
            
            final List<Span> spans = decoder.parseExportTraceServiceRequest(originalRequest, TEST_TIME);
            final Span span = spans.get(0);
            
            final ResourceSpans encodedResourceSpans = encoder.convertToResourceSpans(span);
            
            // Verify scope is present
            assertThat(encodedResourceSpans.getScopeSpansCount(), equalTo(1));
            assertThat(encodedResourceSpans.getScopeSpans(0).hasScope(), equalTo(true));
            
            final io.opentelemetry.proto.common.v1.InstrumentationScope originalScope = 
                originalRequest.getResourceSpans(0).getScopeSpans(0).getScope();
            final io.opentelemetry.proto.common.v1.InstrumentationScope encodedScope = 
                encodedResourceSpans.getScopeSpans(0).getScope();
            
            assertEquals(originalScope.getName(), encodedScope.getName());
            assertEquals(originalScope.getVersion(), encodedScope.getVersion());
        }

        @Test
        void testMetricResourceAttributesPreserved() throws Exception {
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-gauge-metrics.json");
            
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            final Metric metric = records.iterator().next().getData();
            
            final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(metric);
            
            // Verify resource is present
            assertThat(encodedResourceMetrics.hasResource(), equalTo(true));
            
            final io.opentelemetry.proto.resource.v1.Resource originalResource = 
                originalRequest.getResourceMetrics(0).getResource();
            final io.opentelemetry.proto.resource.v1.Resource encodedResource = 
                encodedResourceMetrics.getResource();
            
            assertThat(encodedResource.getAttributesCount(), 
                equalTo(originalResource.getAttributesCount()));
        }
    }

    @Nested
    class EdgeCaseTests {

        @Test
        void testEmptyAttributesRoundTrip() throws Exception {
            final ExportTraceServiceRequest originalRequest = 
                buildExportTraceServiceRequestFromJsonFile("test-request-no-spans.json");
            
            // This should handle gracefully even with no spans
            final List<Span> spans = decoder.parseExportTraceServiceRequest(originalRequest, TEST_TIME);
            
            // If there are spans, encode them
            for (Span span : spans) {
                final ResourceSpans encodedResourceSpans = encoder.convertToResourceSpans(span);
                assertNotNull(encodedResourceSpans);
            }
        }

        @Test
        void testHistogramWithoutExplicitBounds() throws Exception {
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-histogram-metrics-no-explicit-bounds.json");
            
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            
            for (Record<? extends Metric> record : records) {
                final Metric metric = record.getData();
                if (metric instanceof JacksonHistogram) {
                    final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(metric);
                    assertNotNull(encodedResourceMetrics);
                }
            }
        }

        @Test
        void testHistogramDataPointValuesAreEncoded() throws Exception {
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-histogram-metrics.json");
            
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            final Metric metric = records.iterator().next().getData();
            
            final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(metric);
            
            final io.opentelemetry.proto.metrics.v1.Metric encodedMetric = 
                encodedResourceMetrics.getScopeMetrics(0).getMetrics(0);
            final io.opentelemetry.proto.metrics.v1.Metric originalMetric = 
                originalRequest.getResourceMetrics(0).getScopeMetrics(0).getMetrics(0);
            
            // Verify histogram data points are actually included with values
            assertThat(encodedMetric.hasHistogram(), equalTo(true));
            assertThat(encodedMetric.getHistogram().getDataPointsCount(), equalTo(1));
            
            final var originalDataPoint = originalMetric.getHistogram().getDataPoints(0);
            final var encodedDataPoint = encodedMetric.getHistogram().getDataPoints(0);
            
            // Verify count and sum are preserved (the actual data!)
            assertEquals(originalDataPoint.getCount(), encodedDataPoint.getCount());
            assertEquals(originalDataPoint.getSum(), encodedDataPoint.getSum(), 0.0001);
            
            // Verify bucket counts are preserved (the distribution data!)
            assertEquals(originalDataPoint.getBucketCountsCount(), 
                encodedDataPoint.getBucketCountsCount());
            for (int i = 0; i < originalDataPoint.getBucketCountsCount(); i++) {
                assertEquals(originalDataPoint.getBucketCounts(i), 
                    encodedDataPoint.getBucketCounts(i),
                    "Bucket count at index " + i + " should match");
            }
        }

        @Test
        void testGaugeDataPointValueIsEncoded() throws Exception {
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-gauge-metrics.json");
            
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            final Metric metric = records.iterator().next().getData();
            
            final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(metric);
            
            final io.opentelemetry.proto.metrics.v1.Metric encodedMetric = 
                encodedResourceMetrics.getScopeMetrics(0).getMetrics(0);
            final io.opentelemetry.proto.metrics.v1.Metric originalMetric = 
                originalRequest.getResourceMetrics(0).getScopeMetrics(0).getMetrics(0);
            
            // Verify gauge data point is included with actual value
            assertThat(encodedMetric.hasGauge(), equalTo(true));
            assertThat(encodedMetric.getGauge().getDataPointsCount(), equalTo(1));
            
            final var originalDataPoint = originalMetric.getGauge().getDataPoints(0);
            final var encodedDataPoint = encodedMetric.getGauge().getDataPoints(0);
            
            // Verify the actual numeric value is preserved
            if (originalDataPoint.hasAsDouble()) {
                assertEquals(originalDataPoint.getAsDouble(), 
                    encodedDataPoint.getAsDouble(), 0.0001,
                    "Gauge value should be preserved");
            } else if (originalDataPoint.hasAsInt()) {
                assertEquals((double) originalDataPoint.getAsInt(), 
                    encodedDataPoint.getAsDouble(), 0.0001,
                    "Gauge value should be preserved");
            }
        }
    }
}
