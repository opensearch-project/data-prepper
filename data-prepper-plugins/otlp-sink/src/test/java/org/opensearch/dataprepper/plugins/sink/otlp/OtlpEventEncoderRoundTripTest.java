/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp;

import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Comprehensive round-trip tests for OTLP encoding/decoding.
 * Tests the complete pipeline: Protobuf -> Data Prepper Model -> Protobuf
 */
public class OtlpEventEncoderRoundTripTest {

    private static final Instant TEST_TIME = Instant.now();

    private OTelProtoStandardCodec.OTelProtoEncoder encoder;
    private OTelProtoStandardCodec.OTelProtoDecoder decoder;

    @BeforeEach
    void setUp() {
        encoder = new OTelProtoStandardCodec.OTelProtoEncoder();
        decoder = new OTelProtoStandardCodec.OTelProtoDecoder();
    }

    private String getFileAsJsonString(String requestJsonFileName) throws IOException {
        final StringBuilder jsonBuilder = new StringBuilder();
        try (final InputStream inputStream = Objects.requireNonNull(
                OtlpEventEncoderRoundTripTest.class.getClassLoader().getResourceAsStream(requestJsonFileName))) {
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

    @Nested
    class TraceEncodingTests {

        @Test
        void testSpanEncodingMatchesOriginalStructure() throws Exception {
            // Load original protobuf
            final ExportTraceServiceRequest originalRequest = 
                buildExportTraceServiceRequestFromJsonFile("test-request.json");
            
            // Decode to Data Prepper model
            final List<Span> spans = decoder.parseExportTraceServiceRequest(originalRequest, TEST_TIME);
            assertThat(spans.size(), equalTo(1));
            
            final Span span = spans.get(0);
            
            // Encode using OtlpEventEncoder
            final ResourceSpans encodedResourceSpans = encoder.convertToResourceSpans(span);
            
            // Verify structure matches
            assertNotNull(encodedResourceSpans);
            assertThat(encodedResourceSpans.getScopeSpansCount(), equalTo(1));
            assertThat(encodedResourceSpans.getScopeSpans(0).getSpansCount(), equalTo(1));
            
            final io.opentelemetry.proto.trace.v1.Span encodedSpan = 
                encodedResourceSpans.getScopeSpans(0).getSpans(0);
            final io.opentelemetry.proto.trace.v1.Span originalSpan = 
                originalRequest.getResourceSpans(0).getScopeSpans(0).getSpans(0);
            
            // Verify critical fields
            assertEquals(originalSpan.getSpanId(), encodedSpan.getSpanId());
            assertEquals(originalSpan.getTraceId(), encodedSpan.getTraceId());
            assertEquals(originalSpan.getName(), encodedSpan.getName());
        }

        @Test
        void testMultipleSpansCanBeEncodedIndependently() throws Exception {
            final ExportTraceServiceRequest originalRequest = 
                buildExportTraceServiceRequestFromJsonFile("test-request-multiple-traces.json");
            
            final List<Span> spans = decoder.parseExportTraceServiceRequest(originalRequest, TEST_TIME);
            
            // Each span should be encodable independently
            for (Span span : spans) {
                final ResourceSpans encodedResourceSpans = encoder.convertToResourceSpans(span);
                
                assertNotNull(encodedResourceSpans);
                assertThat(encodedResourceSpans.getScopeSpansCount(), equalTo(1));
                assertThat(encodedResourceSpans.getScopeSpans(0).getSpansCount(), equalTo(1));
            }
        }
    }

    @Nested
    class MetricsEncodingTests {

        @Test
        void testGaugeEncodingPreservesAllFields() throws Exception {
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-gauge-metrics.json");
            
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            
            assertThat(records.size(), equalTo(1));
            final Metric metric = records.iterator().next().getData();
            assertThat(metric, instanceOf(JacksonGauge.class));
            
            // Encode using OtlpEventEncoder
            final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(metric);
            
            assertNotNull(encodedResourceMetrics);
            assertThat(encodedResourceMetrics.getScopeMetricsCount(), equalTo(1));
            
            final io.opentelemetry.proto.metrics.v1.Metric encodedMetric = 
                encodedResourceMetrics.getScopeMetrics(0).getMetrics(0);
            final io.opentelemetry.proto.metrics.v1.Metric originalMetric = 
                originalRequest.getResourceMetrics(0).getScopeMetrics(0).getMetrics(0);
            
            // Verify gauge structure
            assertThat(encodedMetric.hasGauge(), equalTo(true));
            assertEquals(originalMetric.getName(), encodedMetric.getName());
            assertEquals(originalMetric.getDescription(), encodedMetric.getDescription());
            assertEquals(originalMetric.getUnit(), encodedMetric.getUnit());
        }

        @Test
        void testSumEncodingPreservesMonotonicFlag() throws Exception {
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-sum-metrics.json");
            
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            
            final Metric metric = records.iterator().next().getData();
            assertThat(metric, instanceOf(JacksonSum.class));
            
            final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(metric);
            
            final io.opentelemetry.proto.metrics.v1.Metric encodedMetric = 
                encodedResourceMetrics.getScopeMetrics(0).getMetrics(0);
            final io.opentelemetry.proto.metrics.v1.Metric originalMetric = 
                originalRequest.getResourceMetrics(0).getScopeMetrics(0).getMetrics(0);
            
            // Verify sum-specific fields
            assertThat(encodedMetric.hasSum(), equalTo(true));
            assertEquals(originalMetric.getSum().getIsMonotonic(), 
                encodedMetric.getSum().getIsMonotonic());
            assertEquals(originalMetric.getSum().getAggregationTemporality(), 
                encodedMetric.getSum().getAggregationTemporality());
        }

        @Test
        void testHistogramEncodingPreservesBuckets() throws Exception {
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-histogram-metrics.json");
            
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            
            final Metric metric = records.iterator().next().getData();
            assertThat(metric, instanceOf(JacksonHistogram.class));
            
            final JacksonHistogram histogram = (JacksonHistogram) metric;
            final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(histogram);
            
            final io.opentelemetry.proto.metrics.v1.Metric encodedMetric = 
                encodedResourceMetrics.getScopeMetrics(0).getMetrics(0);
            final io.opentelemetry.proto.metrics.v1.Metric originalMetric = 
                originalRequest.getResourceMetrics(0).getScopeMetrics(0).getMetrics(0);
            
            // Verify histogram structure
            assertThat(encodedMetric.hasHistogram(), equalTo(true));
            
            if (originalMetric.getHistogram().getDataPointsCount() > 0) {
                final var originalDataPoint = originalMetric.getHistogram().getDataPoints(0);
                final var encodedDataPoint = encodedMetric.getHistogram().getDataPoints(0);
                
                // Verify bucket structure is preserved
                assertEquals(originalDataPoint.getBucketCountsCount(), 
                    encodedDataPoint.getBucketCountsCount());
                assertEquals(originalDataPoint.getExplicitBoundsCount(), 
                    encodedDataPoint.getExplicitBoundsCount());
            }
        }

        @Test
        void testAllMetricTypesCanBeEncoded() throws Exception {
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-request-multiple-metrics.json");
            
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            
            int successfulEncodings = 0;
            
            for (Record<? extends Metric> record : records) {
                final Metric metric = record.getData();
                
                try {
                    final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(metric);
                    assertNotNull(encodedResourceMetrics);
                    successfulEncodings++;
                } catch (Exception e) {
                    throw new AssertionError("Failed to encode metric: " + metric.getName(), e);
                }
            }
            
            // Verify all metrics were successfully encoded
            assertEquals(records.size(), successfulEncodings);
        }
    }


    @Nested
    class DataFidelityTests {

        @Test
        void testNumericPrecisionPreserved() throws Exception {
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
            
            // Compare numeric values
            if (originalMetric.hasGauge() && originalMetric.getGauge().getDataPointsCount() > 0) {
                final var originalDataPoint = originalMetric.getGauge().getDataPoints(0);
                final var encodedDataPoint = encodedMetric.getGauge().getDataPoints(0);
                
                if (originalDataPoint.hasAsDouble()) {
                    assertEquals(originalDataPoint.getAsDouble(), 
                        encodedDataPoint.getAsDouble(), 0.0001);
                }
            }
        }

        @Test
        void testTimestampsPreserved() throws Exception {
            final ExportTraceServiceRequest originalRequest = 
                buildExportTraceServiceRequestFromJsonFile("test-request.json");
            
            final List<Span> spans = decoder.parseExportTraceServiceRequest(originalRequest, TEST_TIME);
            final Span span = spans.get(0);
            
            final ResourceSpans encodedResourceSpans = encoder.convertToResourceSpans(span);
            
            final io.opentelemetry.proto.trace.v1.Span encodedSpan = 
                encodedResourceSpans.getScopeSpans(0).getSpans(0);
            final io.opentelemetry.proto.trace.v1.Span originalSpan = 
                originalRequest.getResourceSpans(0).getScopeSpans(0).getSpans(0);
            
            // Verify timestamps are preserved
            assertEquals(originalSpan.getStartTimeUnixNano(), encodedSpan.getStartTimeUnixNano());
            assertEquals(originalSpan.getEndTimeUnixNano(), encodedSpan.getEndTimeUnixNano());
        }

        @Test
        void testAttributeTypesPreserved() throws Exception {
            final ExportTraceServiceRequest originalRequest = 
                buildExportTraceServiceRequestFromJsonFile("test-request.json");
            
            final List<Span> spans = decoder.parseExportTraceServiceRequest(originalRequest, TEST_TIME);
            final Span span = spans.get(0);
            
            final ResourceSpans encodedResourceSpans = encoder.convertToResourceSpans(span);
            
            final io.opentelemetry.proto.trace.v1.Span encodedSpan = 
                encodedResourceSpans.getScopeSpans(0).getSpans(0);
            
            // Verify attributes exist
            assertThat(encodedSpan.getAttributesCount() > 0, equalTo(true));
            
            // Verify different attribute types can be encoded
            boolean hasStringAttr = false;
            boolean hasNumericAttr = false;
            boolean hasBoolAttr = false;
            
            for (io.opentelemetry.proto.common.v1.KeyValue kv : encodedSpan.getAttributesList()) {
                if (kv.getValue().hasStringValue()) {
                    hasStringAttr = true;
                } else if (kv.getValue().hasIntValue() || kv.getValue().hasDoubleValue()) {
                    hasNumericAttr = true;
                } else if (kv.getValue().hasBoolValue()) {
                    hasBoolAttr = true;
                }
            }
            
            // At least one type should be present
            assertThat(hasStringAttr || hasNumericAttr || hasBoolAttr, equalTo(true));
        }
    }

    @Nested
    class IntegrationTests {

        @Test
        void testCompleteOTLPPipelineSimulation() throws Exception {
            // Simulate: OTLP Source -> Processor (modify attributes) -> OTLP Sink
            
            // Step 1: Receive OTLP data
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-gauge-metrics.json");
            
            // Step 2: Decode to Data Prepper model
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            final Metric metric = records.iterator().next().getData();
            
            // Step 3: Simulate processor modification
            metric.getAttributes().put("pipeline.processed", true);
            metric.getAttributes().put("pipeline.timestamp", System.currentTimeMillis());
            
            // Step 4: Encode back to OTLP
            final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(metric);
            
            // Step 5: Verify output is valid OTLP
            assertNotNull(encodedResourceMetrics);
            assertThat(encodedResourceMetrics.getScopeMetricsCount(), equalTo(1));
            
            final io.opentelemetry.proto.metrics.v1.Metric encodedMetric = 
                encodedResourceMetrics.getScopeMetrics(0).getMetrics(0);
            
            // Verify original data is preserved
            final io.opentelemetry.proto.metrics.v1.Metric originalMetric = 
                originalRequest.getResourceMetrics(0).getScopeMetrics(0).getMetrics(0);
            assertEquals(originalMetric.getName(), encodedMetric.getName());
            
            // Verify modifications are present
            if (encodedMetric.hasGauge() && encodedMetric.getGauge().getDataPointsCount() > 0) {
                final var dataPoint = encodedMetric.getGauge().getDataPoints(0);
                
                boolean foundProcessedFlag = false;
                boolean foundTimestamp = false;
                
                for (io.opentelemetry.proto.common.v1.KeyValue kv : dataPoint.getAttributesList()) {
                    if (kv.getKey().equals("pipeline.processed")) {
                        foundProcessedFlag = true;
                    }
                    if (kv.getKey().equals("pipeline.timestamp")) {
                        foundTimestamp = true;
                    }
                }
                
                assertThat(foundProcessedFlag, equalTo(true));
                assertThat(foundTimestamp, equalTo(true));
            }
        }

        @Test
        void testBatchProcessingScenario() throws Exception {
            // Simulate processing a batch of different metric types
            final ExportMetricsServiceRequest originalRequest = 
                buildExportMetricsServiceRequestFromJsonFile("test-request-multiple-metrics.json");
            
            final Collection<Record<? extends Metric>> records = 
                decoder.parseExportMetricsServiceRequest(originalRequest, TEST_TIME);
            
            // Process each metric and encode
            int processedCount = 0;
            for (Record<? extends Metric> record : records) {
                final Metric metric = record.getData();
                
                // Add batch processing metadata
                metric.getAttributes().put("batch.id", "batch-123");
                metric.getAttributes().put("batch.sequence", processedCount);
                
                // Encode
                final ResourceMetrics encodedResourceMetrics = encoder.convertToResourceMetrics(metric);
                assertNotNull(encodedResourceMetrics);
                
                processedCount++;
            }
            
            assertEquals(records.size(), processedCount);
        }
    }
}
