/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.ArrayValue;
import io.opentelemetry.proto.common.v1.InstrumentationScope;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.common.v1.KeyValueList;
import io.opentelemetry.proto.metrics.v1.Exemplar;
import io.opentelemetry.proto.metrics.v1.ExponentialHistogramDataPoint;
import io.opentelemetry.proto.metrics.v1.NumberDataPoint;
import io.opentelemetry.proto.resource.v1.Resource;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import io.opentelemetry.proto.trace.v1.ScopeSpans;
import io.opentelemetry.proto.trace.v1.Status;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.log.OpenTelemetryLog;
import org.opensearch.dataprepper.model.metric.Bucket;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.metric.JacksonMetric;
import org.opensearch.dataprepper.model.metric.JacksonGauge;
import org.opensearch.dataprepper.model.metric.JacksonSum;
import org.opensearch.dataprepper.model.metric.JacksonHistogram;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultLink;
import org.opensearch.dataprepper.model.trace.DefaultSpanEvent;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Link;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.model.trace.SpanEvent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.entry;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OTelProtoStandardCodecTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Random RANDOM = new Random();
    private static final String TEST_REQUEST_TRACE_JSON_FILE = "test-request.json";
    private static final String TEST_REQUEST_BOTH_SPAN_TYPES_JSON_FILE = "test-request-both-span-types.json";
    private static final String TEST_REQUEST_NO_SPANS_JSON_FILE = "test-request-no-spans.json";
    private static final String TEST_SPAN_EVENT_JSON_FILE = "test-span-event.json";
    private static final String TEST_REQUEST_GAUGE_METRICS_JSON_FILE = "test-gauge-metrics.json";
    private static final String TEST_REQUEST_SUM_METRICS_JSON_FILE = "test-sum-metrics.json";
    private static final String TEST_REQUEST_HISTOGRAM_METRICS_JSON_FILE = "test-histogram-metrics.json";
    private static final String TEST_REQUEST_HISTOGRAM_METRICS_NO_EXPLICIT_BOUNDS_JSON_FILE = "test-histogram-metrics-no-explicit-bounds.json";
    private static final String TEST_REQUEST_LOGS_JSON_FILE = "test-request-log.json";
    private static final String TEST_REQUEST_MULTIPLE_TRACES_FILE = "test-request-multiple-traces.json";


    private static final Long TIME = TimeUnit.MILLISECONDS.toNanos(ZonedDateTime.of(
            LocalDateTime.of(2020, 5, 24, 14, 1, 0),
            ZoneOffset.UTC).toInstant().toEpochMilli());

    private static final Double MAX_ERROR = 0.00001;

    private final OTelProtoStandardCodec.OTelProtoDecoder decoderUnderTest = new OTelProtoStandardCodec.OTelProtoDecoder();
    private final OTelProtoStandardCodec.OTelProtoEncoder encoderUnderTest = new OTelProtoStandardCodec.OTelProtoEncoder();
    private static byte[] getRandomBytes(int len) {
        byte[] bytes = new byte[len];
        RANDOM.nextBytes(bytes);
        return bytes;
    }

    private Map<String, Object> returnMap(final String jsonStr) throws JsonProcessingException {
        return (Map<String, Object>) OBJECT_MAPPER.readValue(jsonStr, Map.class);
    }

    private List<Object> returnList(final String jsonStr) throws JsonProcessingException {
        return (List<Object>) OBJECT_MAPPER.readValue(jsonStr, List.class);
    }

    private ExportTraceServiceRequest buildExportTraceServiceRequestFromJsonFile(String requestJsonFileName) throws IOException {
        final ExportTraceServiceRequest.Builder builder = ExportTraceServiceRequest.newBuilder();
        JsonFormat.parser().merge(getFileAsJsonString(requestJsonFileName), builder);
        return builder.build();
    }

    private ExportLogsServiceRequest buildExportLogsServiceRequestFromJsonFile(String requestJsonFileName) throws IOException {
        final ExportLogsServiceRequest.Builder builder = ExportLogsServiceRequest.newBuilder();
        JsonFormat.parser().merge(getFileAsJsonString(requestJsonFileName), builder);
        return builder.build();
    }

    private ExportMetricsServiceRequest buildExportMetricsServiceRequestFromJsonFile(String requestJsonFileName) throws IOException {
        final ExportMetricsServiceRequest.Builder builder = ExportMetricsServiceRequest.newBuilder();
        JsonFormat.parser().merge(getFileAsJsonString(requestJsonFileName), builder);
        return builder.build();
    }

    private String getFileAsJsonString(String requestJsonFileName) throws IOException {
        final StringBuilder jsonBuilder = new StringBuilder();
        try (final InputStream inputStream = Objects.requireNonNull(
                OTelProtoStandardCodecTest.class.getClassLoader().getResourceAsStream(requestJsonFileName))) {
            final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            bufferedReader.lines().forEach(jsonBuilder::append);
        }
        return jsonBuilder.toString();
    }


    @Nested
    class OTelProtoDecoderTest {
        @Test
        public void testSplitExportTraceServiceRequestWithMultipleTraces() throws Exception {
            final ExportTraceServiceRequest exportTraceServiceRequest = buildExportTraceServiceRequestFromJsonFile(TEST_REQUEST_MULTIPLE_TRACES_FILE);
            final Map<String, ExportTraceServiceRequest> map = decoderUnderTest.splitExportTraceServiceRequestByTraceId(exportTraceServiceRequest);
            assertThat(map.size(), is(equalTo(3)));
            for (Map.Entry<String, ExportTraceServiceRequest> entry: map.entrySet()) {
                String expectedTraceId = new String(Hex.decodeHex(entry.getKey()), StandardCharsets.UTF_8);
                ExportTraceServiceRequest request = entry.getValue();
                if (expectedTraceId.equals("TRACEID1")) {
                    assertThat(request.getResourceSpansList().size(), equalTo(1));
                    ResourceSpans rs = request.getResourceSpansList().get(0);
                    assertThat(rs.getScopeSpansList().size(), equalTo(1));
                    ScopeSpans ss = rs.getScopeSpansList().get(0);
                    assertThat(ss.getSpansList().size(), equalTo(1));
                    io.opentelemetry.proto.trace.v1.Span span = ss.getSpansList().get(0);
                    String spanId = span.getSpanId().toStringUtf8();
                    assertTrue(spanId.equals("TRACEID1-SPAN1"));
                } else if (expectedTraceId.equals("TRACEID2")) {
                    assertThat(request.getResourceSpansList().size(), equalTo(1));
                    ResourceSpans rs = request.getResourceSpansList().get(0);
                    assertThat(rs.getScopeSpansList().size(), equalTo(2));

                    ScopeSpans ss = rs.getScopeSpansList().get(0);
                    assertThat(ss.getSpansList().size(), equalTo(1));
                    io.opentelemetry.proto.trace.v1.Span span = ss.getSpansList().get(0);
                    String spanId = span.getSpanId().toStringUtf8();
                    assertTrue(spanId.equals("TRACEID2-SPAN1"));

                    ss = rs.getScopeSpansList().get(1);
                    assertThat(ss.getSpansList().size(), equalTo(1));
                    span = ss.getSpansList().get(0);
                    spanId = span.getSpanId().toStringUtf8();
                    assertTrue(spanId.equals("TRACEID2-SPAN2"));

                } else if (expectedTraceId.equals("TRACEID3")) {
                    assertThat(request.getResourceSpansList().size(), equalTo(1));
                    ResourceSpans rs = request.getResourceSpansList().get(0);
                    assertThat(rs.getScopeSpansList().size(), equalTo(1));
                    ScopeSpans ss = rs.getScopeSpansList().get(0);
                    assertThat(ss.getSpansList().size(), equalTo(1));
                    io.opentelemetry.proto.trace.v1.Span span = ss.getSpansList().get(0);
                    String spanId = span.getSpanId().toStringUtf8();
                    assertTrue(spanId.equals("TRACEID3-SPAN1"));
                } else {
                    assertTrue("Failed".equals("Unknown TraceId"));
                }
            }
        }

        @Test
        public void testParseExportTraceServiceRequest() throws IOException {
            final ExportTraceServiceRequest exportTraceServiceRequest = buildExportTraceServiceRequestFromJsonFile(TEST_REQUEST_TRACE_JSON_FILE);
            final List<Span> spans = decoderUnderTest.parseExportTraceServiceRequest(exportTraceServiceRequest, Instant.now());
            validateSpans(spans);
        }

        @Test
        public void testParseExportTraceServiceRequest_NoSpans() throws IOException {
            final ExportTraceServiceRequest exportTraceServiceRequest = buildExportTraceServiceRequestFromJsonFile(TEST_REQUEST_NO_SPANS_JSON_FILE);
            final List<Span> spans = decoderUnderTest.parseExportTraceServiceRequest(exportTraceServiceRequest, Instant.now());
            assertThat(spans.size(), is(equalTo(0)));
        }

        private void validateSpans(final List<Span> spans) {
            assertThat(spans.size(), is(equalTo(3)));

            for (final Span span : spans) {
                assertThat(span.getTraceGroup(), nullValue());
                assertThat(span.getTraceGroupFields(), nullValue());
                Map<String, Object> resource = span.getResource();
                assertThat(resource.containsKey(OTelProtoStandardCodec.ATTRIBUTES_KEY), is(true));
                Map<String, Object> attributes = (Map<String, Object>)resource.get(OTelProtoStandardCodec.ATTRIBUTES_KEY);
                assertThat(attributes.containsKey("service.name"), is(true));
                Map<String, Object> scope = span.getScope();
                assertThat(scope.containsKey(OTelProtoStandardCodec.NAME_KEY), is(true));
            }
        }

        @Test
        public void testGetSpanEvent() {
            final String testName = "test name";
            final long testTimeNanos = System.nanoTime();
            final String testTime = OTelProtoCommonUtils.convertUnixNanosToISO8601(testTimeNanos);
            final String testKey = "test key";
            final String testValue = "test value";
            io.opentelemetry.proto.trace.v1.Span.Event testOTelProtoSpanEvent = io.opentelemetry.proto.trace.v1.Span.Event.newBuilder()
                    .setName(testName)
                    .setTimeUnixNano(testTimeNanos)
                    .setDroppedAttributesCount(0)
                    .addAttributes(KeyValue.newBuilder().setKey(testKey).setValue(AnyValue.newBuilder()
                            .setStringValue(testValue).build()).build())
                    .build();
            final SpanEvent result = decoderUnderTest.getSpanEvent(testOTelProtoSpanEvent);
            assertThat(result.getAttributes().size(), equalTo(1));
            assertThat(result.getDroppedAttributesCount(), equalTo(0));
            assertThat(result.getName(), equalTo(testName));
            assertThat(result.getTime(), equalTo(testTime));
        }

        @Test
        public void testGetSpanLink() {
            final byte[] testSpanIdBytes = getRandomBytes(16);
            final byte[] testTraceIdBytes = getRandomBytes(16);
            final String testSpanId = Hex.encodeHexString(testSpanIdBytes);
            final String testTraceId = Hex.encodeHexString(testTraceIdBytes);
            final String testTraceState = "test state";
            final String testKey = "test key";
            final String testValue = "test value";
            io.opentelemetry.proto.trace.v1.Span.Link testOTelProtoSpanLink = io.opentelemetry.proto.trace.v1.Span.Link.newBuilder()
                    .setSpanId(ByteString.copyFrom(testSpanIdBytes))
                    .setTraceId(ByteString.copyFrom(testTraceIdBytes))
                    .setTraceState(testTraceState)
                    .setDroppedAttributesCount(0)
                    .addAttributes(KeyValue.newBuilder().setKey(testKey).setValue(AnyValue.newBuilder()
                            .setStringValue(testValue).build()).build())
                    .build();
            final Link result = decoderUnderTest.getLink(testOTelProtoSpanLink);
            assertThat(result.getAttributes().size(), equalTo(1));
            assertThat(result.getDroppedAttributesCount(), equalTo(0));
            assertThat(result.getSpanId(), equalTo(testSpanId));
            assertThat(result.getTraceId(), equalTo(testTraceId));
            assertThat(result.getTraceState(), equalTo(testTraceState));
        }

        /**
         * Below object has a KeyValue with a key mapped to KeyValueList and is part of the span attributes
         *
         * @throws JsonProcessingException
         */
        @Test
        public void testKeyValueListAsSpanAttributes() throws JsonProcessingException {

            final KeyValue childAttr1 = KeyValue.newBuilder().setKey("statement").setValue(AnyValue.newBuilder()
                    .setIntValue(1_000).build()).build();
            final KeyValue childAttr2 = KeyValue.newBuilder().setKey("statement.params").setValue(AnyValue.newBuilder()
                    .setStringValue("us-east-1").build()).build();
            final KeyValue spanAttribute1 = KeyValue.newBuilder().setKey("db.details").setValue(AnyValue.newBuilder()
                    .setKvlistValue(KeyValueList.newBuilder().addAllValues(Arrays.asList(childAttr1, childAttr2)).build()).build()).build();
            final KeyValue spanAttribute2 = KeyValue.newBuilder().setKey("http.status").setValue(AnyValue.newBuilder()
                    .setStringValue("4xx").build()).build();

            final Map<String, Object> actual = decoderUnderTest.getSpanAttributes(io.opentelemetry.proto.trace.v1.Span.newBuilder()
                    .addAllAttributes(Arrays.asList(spanAttribute1, spanAttribute2)).build());
            assertThat(actual.get(spanAttribute2.getKey()),
                    equalTo(spanAttribute2.getValue().getStringValue()));
            assertThat(actual.containsKey(spanAttribute1.getKey()), is(true));
            final Map<String, Object> actualValue = (Map<String, Object>)actual.get(spanAttribute1.getKey());
            assertThat(((Number) actualValue.get(childAttr1.getKey())).longValue(),
                    equalTo(childAttr1.getValue().getIntValue()));
            assertThat(actualValue.get(childAttr2.getKey()), equalTo(childAttr2.getValue().getStringValue()));
        }

        /**
         * Below object has a KeyValue with a key mapped to KeyValueList and is part of the resource attributes
         *
         * @throws JsonProcessingException
         */
        @Test
        public void testKeyValueListAsResourceAttributes() throws JsonProcessingException {
            final KeyValue childAttr1 = KeyValue.newBuilder().setKey("ec2.instances").setValue(AnyValue.newBuilder()
                    .setIntValue(20).build()).build();
            final KeyValue childAttr2 = KeyValue.newBuilder().setKey("ec2.instance.az").setValue(AnyValue.newBuilder()
                    .setStringValue("us-east-1").build()).build();
            final KeyValue spanAttribute1 = KeyValue.newBuilder().setKey("aws.details").setValue(AnyValue.newBuilder()
                    .setKvlistValue(KeyValueList.newBuilder().addAllValues(Arrays.asList(childAttr1, childAttr2)).build()).build()).build();
            final KeyValue spanAttribute2 = KeyValue.newBuilder().setKey("service.name").setValue(AnyValue.newBuilder()
                    .setStringValue("EaglesService").build()).build();

            final Map<String, Object> resourceAttributes = decoderUnderTest.getResourceAttributes(Resource.newBuilder()
                    .addAllAttributes(Arrays.asList(spanAttribute1, spanAttribute2)).build());
            final Map<String, Object> actual = (Map<String, Object>)resourceAttributes.get(OTelProtoStandardCodec.ATTRIBUTES_KEY);
            assertThat(actual.get(spanAttribute2.getKey()), equalTo(spanAttribute2.getValue().getStringValue()));
            assertThat(actual.containsKey(spanAttribute1.getKey()), is(true));
            final Map<String, Object> actualValue = (Map<String, Object>)actual.get(spanAttribute1.getKey());
            assertThat(((Number) actualValue.get(childAttr1.getKey())).longValue(), equalTo(childAttr1.getValue().getIntValue()));
            assertThat(actualValue.get(childAttr2.getKey()), equalTo(childAttr2.getValue().getStringValue()));

        }


        /**
         * Below object has a KeyValue with a key mapped to KeyValueList and is part of the span attributes
         *
         * @throws JsonProcessingException
         */
        @Test
        public void testArrayOfValueAsResourceAttributes() throws JsonProcessingException {
            final KeyValue childAttr1 = KeyValue.newBuilder().setKey("ec2.instances").setValue(AnyValue.newBuilder()
                    .setIntValue(20).build()).build();
            final KeyValue childAttr2 = KeyValue.newBuilder().setKey("ec2.instance.az").setValue(AnyValue.newBuilder()
                    .setStringValue("us-east-1").build()).build();
            final AnyValue anyValue1 = AnyValue.newBuilder().setStringValue(UUID.randomUUID().toString()).build();
            final AnyValue anyValue2 = AnyValue.newBuilder().setDoubleValue(2000.123).build();
            final AnyValue anyValue3 = AnyValue.newBuilder().setKvlistValue(KeyValueList.newBuilder().addAllValues(Arrays.asList(childAttr1, childAttr2))).build();
            final ArrayValue arrayValue = ArrayValue.newBuilder().addAllValues(Arrays.asList(anyValue1, anyValue2, anyValue3)).build();
            final KeyValue spanAttribute1 = KeyValue.newBuilder().setKey("aws.details").setValue(AnyValue.newBuilder()
                    .setArrayValue(arrayValue)).build();

            final Map<String, Object> resource = decoderUnderTest.getResourceAttributes(Resource.newBuilder()
                    .addAllAttributes(Collections.singletonList(spanAttribute1)).build());
            final Map<String, Object> actual = (Map<String, Object>)resource.get(OTelProtoStandardCodec.ATTRIBUTES_KEY);
            assertThat(actual.containsKey(spanAttribute1.getKey()), is(true));
            final List<Object> actualValue = (List<Object>)actual.get(spanAttribute1.getKey());
            assertThat(actualValue.get(0), equalTo(anyValue1.getStringValue()));
            assertThat(((Double) actualValue.get(1)), equalTo(anyValue2.getDoubleValue()));
            final Map<String, Object> map = (Map<String, Object>) actualValue.get(2);
            assertThat(((Number) map.get(childAttr1.getKey())).longValue(), equalTo(childAttr1.getValue().getIntValue()));
            assertThat(map.get(childAttr2.getKey()), equalTo(childAttr2.getValue().getStringValue()));
            assertThat(((Number) map.get(childAttr1.getKey())).longValue(), equalTo(childAttr1.getValue().getIntValue()));

        }


        @Test
        public void testStatusAttributes() {
            final Status st1 = Status.newBuilder().setCode(Status.StatusCode.STATUS_CODE_ERROR).setMessage("Some message").build();
            final Status st2 = Status.newBuilder().setMessage("error message").build();
            final Status st3 = Status.newBuilder().setCode(Status.StatusCode.STATUS_CODE_UNSET).build();
            final Status st4 = Status.newBuilder().build();

            assertThat(decoderUnderTest.getSpanStatusAttributes(st1).size(), equalTo(2));
            assertThat(Status.StatusCode.forNumber((Integer) decoderUnderTest.getSpanStatusAttributes(st1).get(OTelProtoStandardCodec.STATUS_CODE_KEY)), equalTo(st1.getCode()));
            assertThat(decoderUnderTest.getSpanStatusAttributes(st1).get(OTelProtoStandardCodec.STATUS_MESSAGE_KEY), equalTo(st1.getMessage()));

            assertThat(decoderUnderTest.getSpanStatusAttributes(st2).size(), equalTo(2));
            assertThat(Status.StatusCode.forNumber((Integer) decoderUnderTest.getSpanStatusAttributes(st2).get(OTelProtoStandardCodec.STATUS_CODE_KEY)), equalTo(st2.getCode()));

            assertThat(decoderUnderTest.getSpanStatusAttributes(st3).size(), equalTo(1));
            assertThat(Status.StatusCode.forNumber((Integer) decoderUnderTest.getSpanStatusAttributes(st3).get(OTelProtoStandardCodec.STATUS_CODE_KEY)), equalTo(st3.getCode()));

            assertThat(decoderUnderTest.getSpanStatusAttributes(st4).size(), equalTo(1));
            assertThat(Status.StatusCode.forNumber((Integer) decoderUnderTest.getSpanStatusAttributes(st4).get(OTelProtoStandardCodec.STATUS_CODE_KEY)), equalTo(st4.getCode()));

        }

        @Test
        public void testISO8601() {
            final long NANO_MULTIPLIER = 1_000 * 1_000 * 1_000;
            final io.opentelemetry.proto.trace.v1.Span startTimeUnixNano = io.opentelemetry.proto.trace.v1.Span.newBuilder()
                    .setStartTimeUnixNano(651242400000000321L).build();
            final io.opentelemetry.proto.trace.v1.Span endTimeUnixNano = io.opentelemetry.proto.trace.v1.Span.newBuilder()
                    .setEndTimeUnixNano(1598013600000000321L).build();
            final io.opentelemetry.proto.trace.v1.Span emptyTimeSpan = io.opentelemetry.proto.trace.v1.Span.newBuilder().build();

            final String startTime = decoderUnderTest.getStartTimeISO8601(startTimeUnixNano);
            assertThat(Instant.parse(startTime).getEpochSecond() * NANO_MULTIPLIER + Instant.parse(startTime).getNano(), equalTo(startTimeUnixNano.getStartTimeUnixNano()));
            final String endTime = decoderUnderTest.getEndTimeISO8601(endTimeUnixNano);
            assertThat(Instant.parse(endTime).getEpochSecond() * NANO_MULTIPLIER + Instant.parse(endTime).getNano(), equalTo(endTimeUnixNano.getEndTimeUnixNano()));
            final String emptyTime = decoderUnderTest.getStartTimeISO8601(endTimeUnixNano);
            assertThat(Instant.parse(emptyTime).getEpochSecond() * NANO_MULTIPLIER + Instant.parse(emptyTime).getNano(), equalTo(emptyTimeSpan.getStartTimeUnixNano()));

        }

        @Test
        public void testTraceGroup() {
            final io.opentelemetry.proto.trace.v1.Span span1 = io.opentelemetry.proto.trace.v1.Span.newBuilder()
                    .setParentSpanId(ByteString.copyFrom("PArentIdExists", StandardCharsets.UTF_8)).build();
            assertThat(decoderUnderTest.getTraceGroup(span1), nullValue());
            final String testTraceGroup = "testTraceGroup";
            final io.opentelemetry.proto.trace.v1.Span span2 = io.opentelemetry.proto.trace.v1.Span.newBuilder()
                    .setName(testTraceGroup).build();
            assertThat(decoderUnderTest.getTraceGroup(span2), equalTo(testTraceGroup));
        }

        @Test
        public void testParseExportLogsServiceRequest_ScopedLogs() throws IOException {
            final ExportLogsServiceRequest exportLogsServiceRequest = buildExportLogsServiceRequestFromJsonFile(TEST_REQUEST_LOGS_JSON_FILE);
            List<OpenTelemetryLog> logs = decoderUnderTest.parseExportLogsServiceRequest(exportLogsServiceRequest, Instant.now());

            assertThat(logs.size() , is(equalTo(1)));
            validateLog(logs.get(0));
        }

        private void validateLog(OpenTelemetryLog logRecord) {
            assertThat(logRecord.getServiceName(), is("service"));
            assertThat(logRecord.getTime(), is("2020-05-24T14:00:00Z"));
            assertThat(logRecord.getObservedTime(), is("2020-05-24T14:00:02Z"));
            assertThat(logRecord.getBody(), is("Log value"));
            assertThat(logRecord.getDroppedAttributesCount(), is(3));
            assertThat(logRecord.getSchemaUrl(), is("schemaurl"));
            assertThat(logRecord.getSeverityNumber(), is(5));
            assertThat(logRecord.getSeverityText(), is("Severity value"));
            assertThat(logRecord.getTraceId(), is("ba1a1c23b4093b63"));
            assertThat(logRecord.getSpanId(), is("2cc83ac90ebc469c"));
            Map<String, Object> scope = logRecord.getScope();
            Map<String, Object> scopeAttributes = (Map<String, Object>)scope.get(OTelProtoStandardCodec.ATTRIBUTES_KEY);
            assertThat(scopeAttributes.get("my.scope.attribute"), is("log scope attribute"));
            assertThat(scope.get(OTelProtoStandardCodec.NAME_KEY), is("my.library"));
            assertThat(scope.get(OTelProtoStandardCodec.VERSION_KEY), is("1.0.0"));

            Map<String, Object> attributes = logRecord.getAttributes();
            assertThat(attributes.get("statement.params"), is("us-east-1"));
            Map<String, Object> resource = logRecord.getResource();
            Map<String, Object> resourceAttributes = (Map<String, Object>)resource.get(OTelProtoStandardCodec.ATTRIBUTES_KEY);
            assertThat(resourceAttributes.get("service.name"), is("service"));
        }

        @Test
        public void testParseExportMetricsServiceRequest_Guage() throws IOException {
            final ExportMetricsServiceRequest exportMetricsServiceRequest = buildExportMetricsServiceRequestFromJsonFile(TEST_REQUEST_GAUGE_METRICS_JSON_FILE);
            AtomicInteger droppedCount = new AtomicInteger(0);
            final Collection<Record<? extends Metric>> metrics = decoderUnderTest.parseExportMetricsServiceRequest(exportMetricsServiceRequest, droppedCount, 10, Instant.now(), true, true, true);

            validateGaugeMetricRequest(metrics);
        }

        @Test
        public void testParseExportMetricsServiceRequest_Sum() throws IOException {
            final ExportMetricsServiceRequest exportMetricsServiceRequest = buildExportMetricsServiceRequestFromJsonFile(TEST_REQUEST_SUM_METRICS_JSON_FILE);
            AtomicInteger droppedCount = new AtomicInteger(0);
            final Collection<Record<? extends Metric>> metrics = decoderUnderTest.parseExportMetricsServiceRequest(exportMetricsServiceRequest, droppedCount, 10, Instant.now(), true, true, true);
            validateSumMetricRequest(metrics);
        }

        @Test
        public void testParseExportMetricsServiceRequest_Histogram() throws IOException {
            final ExportMetricsServiceRequest exportMetricsServiceRequest = buildExportMetricsServiceRequestFromJsonFile(TEST_REQUEST_HISTOGRAM_METRICS_JSON_FILE);
            AtomicInteger droppedCount = new AtomicInteger(0);
            final Collection<Record<? extends Metric>> metrics = decoderUnderTest.parseExportMetricsServiceRequest(exportMetricsServiceRequest, droppedCount, 10, Instant.now(), true, true, true);
            validateHistogramMetricRequest(metrics);
        }

        @Test
        public void testParseExportMetricsServiceRequest_Histogram_WithNoExplicitBounds() throws IOException {
            final ExportMetricsServiceRequest exportMetricsServiceRequest = buildExportMetricsServiceRequestFromJsonFile(TEST_REQUEST_HISTOGRAM_METRICS_NO_EXPLICIT_BOUNDS_JSON_FILE);
            AtomicInteger droppedCount = new AtomicInteger(0);
            final Collection<Record<? extends Metric>> metrics = decoderUnderTest.parseExportMetricsServiceRequest(exportMetricsServiceRequest, droppedCount, 10, Instant.now(), true, true, true);
            validateHistogramMetricRequestNoExplicitBounds(metrics);
        }

        private void validateGaugeMetricRequest(Collection<Record<? extends Metric>> metrics) {
            assertThat(metrics.size(), equalTo(1));
            Record<? extends Metric> record = ((List<Record<? extends Metric>>)metrics).get(0);
            JacksonMetric metric = (JacksonMetric) record.getData();
            assertThat(metric.getKind(), equalTo(Metric.KIND.GAUGE.toString()));
            assertThat(metric.getUnit(), equalTo("1"));
            assertThat(metric.getName(), equalTo("counter-int"));
            JacksonGauge gauge = (JacksonGauge)metric;
            assertThat(gauge.getValue(), equalTo(123.0));
            Map<String, Object> scope = gauge.getScope();
            Map<String, Object> scopeAttributes = (Map<String, Object>)scope.get(OTelProtoStandardCodec.ATTRIBUTES_KEY);
            assertThat(scopeAttributes.get("my.scope.attribute"), is("gauge scope attribute"));
            assertThat(scope.get(OTelProtoStandardCodec.NAME_KEY), is("my.library"));
            assertThat(scope.get(OTelProtoStandardCodec.VERSION_KEY), is("1.0.0"));
        }

        private void validateSumMetricRequest(Collection<Record<? extends Metric>> metrics) {
            assertThat(metrics.size(), equalTo(1));
            Record<? extends Metric> record = ((List<Record<? extends Metric>>)metrics).get(0);
            JacksonMetric metric = (JacksonMetric) record.getData();
            assertThat(metric.getKind(), equalTo(Metric.KIND.SUM.toString()));
            assertThat(metric.getUnit(), equalTo("1"));
            assertThat(metric.getName(), equalTo("sum-int"));
            JacksonSum sum = (JacksonSum)metric;
            assertThat(sum.getValue(), equalTo(456.0));
            Map<String, Object> scope = sum.getScope();
            Map<String, Object> scopeAttributes = (Map<String, Object>)scope.get(OTelProtoStandardCodec.ATTRIBUTES_KEY);
            assertThat(scopeAttributes.get("my.scope.attribute"), is("sum scope attribute"));
            assertThat(scope.get(OTelProtoStandardCodec.NAME_KEY), is("my.library"));
            assertThat(scope.get(OTelProtoStandardCodec.VERSION_KEY), is("1.0.0"));
        }

        private void validateHistogramMetricRequest(Collection<Record<? extends Metric>> metrics) {
            assertThat(metrics.size(), equalTo(1));
            Record<? extends Metric> record = ((List<Record<? extends Metric>>)metrics).get(0);
            JacksonMetric metric = (JacksonMetric) record.getData();
            assertThat(metric.getKind(), equalTo(Metric.KIND.HISTOGRAM.toString()));
            assertThat(metric.getUnit(), equalTo("1"));
            assertThat(metric.getName(), equalTo("histogram-int"));
            JacksonHistogram histogram = (JacksonHistogram)metric;
            assertThat(histogram.getSum(), equalTo(100.0));
            assertThat(histogram.getCount(), equalTo(30L));
            assertThat(histogram.getExemplars(), equalTo(Collections.emptyList()));
            assertThat(histogram.getExplicitBoundsList(), equalTo(List.of(1.0, 2.0, 3.0, 4.0)));
            assertThat(histogram.getExplicitBoundsCount(), equalTo(4));
            assertThat(histogram.getBucketCountsList(), equalTo(List.of(3L, 5L, 15L, 6L, 1L)));
            assertThat(histogram.getBucketCount(), equalTo(5));
            assertThat(histogram.getAggregationTemporality(), equalTo("AGGREGATION_TEMPORALITY_CUMULATIVE"));
            Map<String, Object> scope = histogram.getScope();
            Map<String, Object> scopeAttributes = (Map<String, Object>)scope.get(OTelProtoStandardCodec.ATTRIBUTES_KEY);
            assertThat(scopeAttributes.get("my.scope.attribute"), is("histogram scope attribute"));
            assertThat(scope.get(OTelProtoStandardCodec.NAME_KEY), is("my.library"));
            assertThat(scope.get(OTelProtoStandardCodec.VERSION_KEY), is("1.0.0"));
        }

        private void validateHistogramMetricRequestNoExplicitBounds(Collection<Record<? extends Metric>> metrics) {
            assertThat(metrics.size(), equalTo(1));
            Record<? extends Metric> record = ((List<Record<? extends Metric>>)metrics).get(0);
            JacksonMetric metric = (JacksonMetric) record.getData();
            assertThat(metric.getKind(), equalTo(Metric.KIND.HISTOGRAM.toString()));
            assertThat(metric.getUnit(), equalTo("1"));
            assertThat(metric.getName(), equalTo("histogram-int"));
            JacksonHistogram histogram = (JacksonHistogram)metric;
            assertThat(histogram.getSum(), equalTo(100.0));
            assertThat(histogram.getCount(), equalTo(30L));
            assertThat(histogram.getExemplars(), equalTo(Collections.emptyList()));
            assertThat(histogram.getExplicitBoundsList(), equalTo(List.of()));
            assertThat(histogram.getExplicitBoundsCount(), equalTo(0));
            assertThat(histogram.getBucketCountsList(), equalTo(List.of(10L)));
            assertThat(histogram.getBucketCount(), equalTo(1));
            assertThat(histogram.getAggregationTemporality(), equalTo("AGGREGATION_TEMPORALITY_CUMULATIVE"));
            Map<String, Object> scope = histogram.getScope();
            Map<String, Object> scopeAttributes = (Map<String, Object>)scope.get(OTelProtoStandardCodec.ATTRIBUTES_KEY);
            assertThat(scopeAttributes.get("my.scope.attribute"), is("histogram scope attribute"));
            assertThat(scope.get(OTelProtoStandardCodec.NAME_KEY), is("my.library"));
            assertThat(scope.get(OTelProtoStandardCodec.VERSION_KEY), is("1.0.0"));
        }


    }

    @Nested
    class OTelProtoEncoderTest {
        @Test
        public void testNullToAnyValue() throws UnsupportedEncodingException {
            final AnyValue result = encoderUnderTest.objectToAnyValue(null);
            assertThat(result.getValueCase(), equalTo(AnyValue.ValueCase.VALUE_NOT_SET));
        }

        @Test
        public void testIntegerToAnyValue() throws UnsupportedEncodingException {
            final Integer testInteger = 1;
            final AnyValue result = encoderUnderTest.objectToAnyValue(testInteger);
            assertThat(result.getValueCase(), equalTo(AnyValue.ValueCase.INT_VALUE));
            assertThat(result.getIntValue(), equalTo(testInteger.longValue()));
        }

        @Test
        public void testLongToAnyValue() throws UnsupportedEncodingException {
            final Long testLong = 1L;
            final AnyValue result = encoderUnderTest.objectToAnyValue(testLong);
            assertThat(result.getValueCase(), equalTo(AnyValue.ValueCase.INT_VALUE));
            assertThat(result.getIntValue(), equalTo(testLong));
        }

        @Test
        public void testBooleanToAnyValue() throws UnsupportedEncodingException {
            final Boolean testBoolean = false;
            final AnyValue result = encoderUnderTest.objectToAnyValue(testBoolean);
            assertThat(result.getValueCase(), equalTo(AnyValue.ValueCase.BOOL_VALUE));
            assertThat(result.getBoolValue(), is(testBoolean));
        }

        @Test
        public void testStringToAnyValue() throws UnsupportedEncodingException {
            final String testString = "test string";
            final AnyValue result = encoderUnderTest.objectToAnyValue(testString);
            assertThat(result.getValueCase(), equalTo(AnyValue.ValueCase.STRING_VALUE));
            assertThat(result.getStringValue(), equalTo(testString));
        }

        @Test
        public void testUnsupportedTypeToAnyValue() {
            assertThrows(UnsupportedEncodingException.class,
                    () -> encoderUnderTest.objectToAnyValue(new UnsupportedEncodingClass()));
        }

        @Test
        public void testSpanAttributesToKeyValueList() throws UnsupportedEncodingException {
            final String testKeyRelevant = "relevantKey";
            final String testKeyIrrelevant = "irrelevantKey";
            /*
            final Map<String, Object> testAllAttributes = Map.of(
                    OTelProtoStandardCodec.OTelProtoEncoder.SPAN_ATTRIBUTES_PREFIX + testKeyRelevant, 1,
                    testKeyIrrelevant, 2);
            final List<KeyValue> result = encoderUnderTest.getSpanAttributes(testAllAttributes);
            assertThat(result.size(), equalTo(1));
            */
            //assertThat(result.get(0).getKey(), equalTo(testKeyRelevant));
            //assertThat(result.get(0).getValue().getIntValue(), equalTo(1L));
        }

        @Test
        public void testResourceAttributesToKeyValueList() throws UnsupportedEncodingException {
            final String testKeyRelevant = "relevantKey";
            final String testKeyIrrelevant = "irrelevantKey";
            /*
            final Map<String, Object> testAllAttributes = Map.of(
                    OTelProtoStandardCodec.OTelProtoEncoder.RESOURCE_ATTRIBUTES_PREFIX + testKeyRelevant, 1,
                    OTelProtoStandardCodec.OTelProtoEncoder.RESOURCE_ATTRIBUTES_PREFIX + OTelProtoStandardCodec.OTelProtoEncoder.SERVICE_NAME_ATTRIBUTE, "A",
                    testKeyIrrelevant, 2);
            final List<KeyValue> result = encoderUnderTest.getResourceAttributes(testAllAttributes);
            assertThat(result.size(), equalTo(1));
            */
            //assertThat(result.get(0).getKey(), equalTo(testKeyRelevant));
            //assertThat(result.get(0).getValue().getIntValue(), equalTo(1L));
        }

        @Test
        public void testEncodeSpanStatusComplete() {
            final String testStatusMessage = "test message";
            final int testStatusCode = Status.StatusCode.STATUS_CODE_OK.getNumber();
            final String testKeyIrrelevant = "irrelevantKey";
            final Map<String, Object> testAllAttributes = Map.of(
                    OTelProtoStandardCodec.STATUS_CODE_KEY, testStatusCode,
                    OTelProtoStandardCodec.STATUS_MESSAGE_KEY, testStatusMessage,
                    testKeyIrrelevant, 2);
            final Status status = encoderUnderTest.constructSpanStatus(testAllAttributes);
            assertThat(status.getCodeValue(), equalTo(testStatusCode));
            assertThat(status.getMessage(), equalTo(testStatusMessage));
        }

        @Test
        public void testEncodeSpanStatusMissingStatusMessage() {
            final int testStatusCode = Status.StatusCode.STATUS_CODE_OK.getNumber();
            final String testKeyIrrelevant = "irrelevantKey";
            final Map<String, Object> testAllAttributes = Map.of(
                    OTelProtoStandardCodec.STATUS_CODE_KEY, testStatusCode,
                    testKeyIrrelevant, 2);
            final Status status = encoderUnderTest.constructSpanStatus(testAllAttributes);
            assertThat(status.getCodeValue(), equalTo(testStatusCode));
        }

        @Test
        public void testEncodeSpanStatusMissingStatusCode() {
            final String testStatusMessage = "test message";
            final String testKeyIrrelevant = "irrelevantKey";
            final Map<String, Object> testAllAttributes = Map.of(
                    OTelProtoStandardCodec.STATUS_MESSAGE_KEY, testStatusMessage,
                    testKeyIrrelevant, 2);
            final Status status = encoderUnderTest.constructSpanStatus(testAllAttributes);
            assertThat(status.getMessage(), equalTo(testStatusMessage));
        }

        @Test
        public void testEncodeSpanStatusMissingAll() {
            final String testKeyIrrelevant = "irrelevantKey";
            final Map<String, Object> testAllAttributes = Map.of(testKeyIrrelevant, 2);
            final Status status = encoderUnderTest.constructSpanStatus(testAllAttributes);
            assertThat(status, instanceOf(Status.class));
        }

        @Test
        public void testEncodeInstrumentationScopeComplete() throws UnsupportedEncodingException, DecoderException {
            final String testName = "test name";
            final String testVersion = "1.1";
            final String testKeyIrrelevant = "irrelevantKey";
            final Map<String, Object> testAllAttributes = Map.of(
                    OTelProtoStandardCodec.NAME_KEY, testName,
                    OTelProtoStandardCodec.VERSION_KEY, testVersion,
                    testKeyIrrelevant, 2);
            final InstrumentationScope instrumentationScope = encoderUnderTest.constructInstrumentationScope(testAllAttributes);
            assertThat(instrumentationScope.getName(), equalTo(testName));
            assertThat(instrumentationScope.getVersion(), equalTo(testVersion));
        }

        @Test
        public void testEncodeInstrumentationScopeMissingName() throws UnsupportedEncodingException, DecoderException {
            final String testVersion = "1.1";
            final String testKeyIrrelevant = "irrelevantKey";
            final Map<String, Object> testAllAttributes = Map.of(
                    OTelProtoStandardCodec.VERSION_KEY, testVersion,
                    testKeyIrrelevant, 2);
            final InstrumentationScope instrumentationScope = encoderUnderTest.constructInstrumentationScope(testAllAttributes);
            assertThat(instrumentationScope.getVersion(), equalTo(testVersion));
        }

        @Test
        public void testEncodeInstrumentationScopeMissingVersion() throws UnsupportedEncodingException, DecoderException {
            final String testName = "test name";
            final String testKeyIrrelevant = "irrelevantKey";
            final Map<String, Object> testAllAttributes = Map.of(
                    OTelProtoStandardCodec.NAME_KEY, testName,
                    testKeyIrrelevant, 2);
            final InstrumentationScope instrumentationScope = encoderUnderTest.constructInstrumentationScope(testAllAttributes);
            assertThat(instrumentationScope.getName(), equalTo(testName));
        }

        @Test
        public void testEncodeInstrumentationScopeMissingAll() throws UnsupportedEncodingException, DecoderException {
            final String testKeyIrrelevant = "irrelevantKey";
            final Map<String, Object> testAllAttributes = Map.of(testKeyIrrelevant, 2);
            final InstrumentationScope instrumentationScope = encoderUnderTest.constructInstrumentationScope(testAllAttributes);
            assertThat(instrumentationScope, instanceOf(InstrumentationScope.class));
        }

        @Test
        public void testEncodeResourceComplete() throws UnsupportedEncodingException {
            final String testServiceName = "test name";
            final String testKeyRelevant = "relevantKey";
            final String testKeyIrrelevant = "irrelevantKey";
            /*
            final Map<String, Object> testAllAttributes = Map.of(
                    OTelProtoStandardCodec.OTelProtoEncoder.RESOURCE_ATTRIBUTES_PREFIX + testKeyRelevant, 1,
                    OTelProtoStandardCodec.OTelProtoEncoder.RESOURCE_ATTRIBUTES_PREFIX + OTelProtoStandardCodec.OTelProtoEncoder.SERVICE_NAME_ATTRIBUTE, "A",
                    testKeyIrrelevant, 2);
            final Resource resource = encoderUnderTest.constructResource(testServiceName, testAllAttributes);
            assertThat(resource.getAttributesCount(), equalTo(2));
            */
/*
            assertThat(
                    resource.getAttributesList().stream()
                            .anyMatch(kv -> kv.getKey().equals(OTelProtoStandardCodec.SERVICE_NAME) && kv.getValue().getStringValue().equals(testServiceName)),
                    is(true));
            assertThat(resource.getAttributesList().stream().noneMatch(kv -> kv.getKey().equals(OTelProtoStandardCodec.OTelProtoEncoder.SERVICE_NAME_ATTRIBUTE)), is(true));
            */
        }

        @Test
        public void testEncodeResourceMissingServiceName() throws UnsupportedEncodingException {
            final String testKeyRelevant = "relevantKey";
            final String testKeyIrrelevant = "irrelevantKey";
            /*
            final Map<String, Object> testAllAttributes = Map.of(
                    OTelProtoStandardCodec.OTelProtoEncoder.RESOURCE_ATTRIBUTES_PREFIX + testKeyRelevant, 1,
                    testKeyIrrelevant, 2);
            final Resource resource = encoderUnderTest.constructResource(null, testAllAttributes);
            assertThat(resource.getAttributesCount(), equalTo(1));
            */
            //assertThat(resource.getAttributesList().stream().noneMatch(kv -> kv.getKey().equals(OTelProtoStandardCodec.SERVICE_NAME)), is(true));
        }

        @Test
        public void testEncodeSpanEvent() throws UnsupportedEncodingException {
            final String testName = "test name";
            final long testTimeNanos = System.nanoTime();
            final String testTime = OTelProtoCommonUtils.convertUnixNanosToISO8601(testTimeNanos);
            final String testKey = "test key";
            final String testValue = "test value";
            final SpanEvent testSpanEvent = DefaultSpanEvent.builder()
                    .withName(testName)
                    .withDroppedAttributesCount(0)
                    .withTime(testTime)
                    .withAttributes(Map.of(testKey, testValue))
                    .build();
            final io.opentelemetry.proto.trace.v1.Span.Event result = encoderUnderTest.convertSpanEvent(testSpanEvent);
            assertThat(result.getAttributesCount(), equalTo(1));
            assertThat(result.getDroppedAttributesCount(), equalTo(0));
            assertThat(result.getName(), equalTo(testName));
            assertThat(result.getTimeUnixNano(), equalTo(testTimeNanos));
        }

        @Test
        public void testEncodeSpanLink() throws DecoderException, UnsupportedEncodingException {
            final byte[] testSpanIdBytes = getRandomBytes(16);
            final byte[] testTraceIdBytes = getRandomBytes(16);
            final String testSpanId = Hex.encodeHexString(testSpanIdBytes);
            final String testTraceId = Hex.encodeHexString(testTraceIdBytes);
            final String testTraceState = "test state";
            final String testKey = "test key";
            final String testValue = "test value";
            final Link testSpanLink = DefaultLink.builder()
                    .withSpanId(testSpanId)
                    .withTraceId(testTraceId)
                    .withTraceState(testTraceState)
                    .withDroppedAttributesCount(0)
                    .withAttributes(Map.of(testKey, testValue))
                    .build();
            final io.opentelemetry.proto.trace.v1.Span.Link result = encoderUnderTest.convertSpanLink(testSpanLink);
            assertThat(result.getAttributesCount(), equalTo(1));
            assertThat(result.getDroppedAttributesCount(), equalTo(0));
            assertThat(result.getSpanId().toByteArray(), equalTo(testSpanIdBytes));
            assertThat(result.getTraceId().toByteArray(), equalTo(testTraceIdBytes));
            assertThat(result.getTraceState(), equalTo(testTraceState));
        }

        @Test
        public void testEncodeResourceSpans() throws DecoderException, UnsupportedEncodingException {
            final Span testSpan = buildSpanFromJsonFile(TEST_SPAN_EVENT_JSON_FILE);
            final ResourceSpans rs = encoderUnderTest.convertToResourceSpans(testSpan);
            assertThat(rs.getResource(), equalTo(Resource.getDefaultInstance()));
            assertThat(rs.getScopeSpansCount(), equalTo(1));
            final ScopeSpans scopeSpans = rs.getScopeSpans(0);
            assertThat(scopeSpans.getScope(), equalTo(InstrumentationScope.getDefaultInstance()));
            assertThat(scopeSpans.getSpansCount(), equalTo(1));
            final io.opentelemetry.proto.trace.v1.Span otelProtoSpan = scopeSpans.getSpans(0);
            assertThat(otelProtoSpan.getTraceId(), equalTo(ByteString.copyFrom(Hex.decodeHex(testSpan.getTraceId()))));
            assertThat(otelProtoSpan.getSpanId(), equalTo(ByteString.copyFrom(Hex.decodeHex(testSpan.getSpanId()))));
            assertThat(otelProtoSpan.getParentSpanId(), equalTo(ByteString.copyFrom(Hex.decodeHex(testSpan.getParentSpanId()))));
            assertThat(otelProtoSpan.getName(), equalTo(testSpan.getName()));
            assertThat(otelProtoSpan.getKind(), equalTo(io.opentelemetry.proto.trace.v1.Span.SpanKind.valueOf(testSpan.getKind())));
            assertThat(otelProtoSpan.getTraceState(), equalTo(testSpan.getTraceState()));
            assertThat(otelProtoSpan.getEventsCount(), equalTo(0));
            assertThat(otelProtoSpan.getDroppedEventsCount(), equalTo(0));
            assertThat(otelProtoSpan.getLinksCount(), equalTo(0));
            assertThat(otelProtoSpan.getDroppedLinksCount(), equalTo(0));
            assertThat(otelProtoSpan.getAttributesCount(), equalTo(0));
            assertThat(otelProtoSpan.getDroppedAttributesCount(), equalTo(0));
        }

        private Span buildSpanFromJsonFile(final String jsonFileName) {
            JacksonSpan.Builder spanBuilder = JacksonSpan.builder();
            try (final InputStream inputStream = Objects.requireNonNull(
                    OTelProtoStandardCodecTest.class.getClassLoader().getResourceAsStream(jsonFileName))) {
                final Map<String, Object> spanMap = OBJECT_MAPPER.readValue(inputStream, new TypeReference<Map<String, Object>>() {
                });
                final String traceId = (String) spanMap.get("traceId");
                final String spanId = (String) spanMap.get("spanId");
                final String parentSpanId = (String) spanMap.get("parentSpanId");
                final String traceState = (String) spanMap.get("traceState");
                final String name = (String) spanMap.get("name");
                final String kind = (String) spanMap.get("kind");
                final Long durationInNanos = ((Number) spanMap.get("durationInNanos")).longValue();
                final String startTime = (String) spanMap.get("startTime");
                final String endTime = (String) spanMap.get("endTime");
                spanBuilder = spanBuilder
                        .withTraceId(traceId)
                        .withSpanId(spanId)
                        .withParentSpanId(parentSpanId)
                        .withTraceState(traceState)
                        .withName(name)
                        .withKind(kind)
                        .withDurationInNanos(durationInNanos)
                        .withStartTime(startTime)
                        .withEndTime(endTime)
                        .withTraceGroup(null);
                DefaultTraceGroupFields.Builder traceGroupFieldsBuilder = DefaultTraceGroupFields.builder();
                if (parentSpanId.isEmpty()) {
                    final Integer statusCode = (Integer) ((Map<String, Object>) spanMap.get("traceGroupFields")).get("statusCode");
                    traceGroupFieldsBuilder = traceGroupFieldsBuilder
                            .withStatusCode(statusCode)
                            .withEndTime(endTime)
                            .withDurationInNanos(durationInNanos);
                    final String traceGroup = (String) spanMap.get("traceGroup");
                    spanBuilder = spanBuilder.withTraceGroup(traceGroup);
                }
                spanBuilder.withTraceGroupFields(traceGroupFieldsBuilder.build());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return spanBuilder.build();
        }

        private class UnsupportedEncodingClass {
        }
    }

    @Test
    public void testTimeCodec() {
        final long testNanos = System.nanoTime();
        final String timeISO8601 = OTelProtoCommonUtils.convertUnixNanosToISO8601(testNanos);
        final long nanoCodecResult = OTelProtoCommonUtils.timeISO8601ToNanos(OTelProtoCommonUtils.convertUnixNanosToISO8601(testNanos));
        assertThat(nanoCodecResult, equalTo(testNanos));
        final String stringCodecResult = OTelProtoCommonUtils.convertUnixNanosToISO8601(OTelProtoCommonUtils.timeISO8601ToNanos(timeISO8601));
        assertThat(stringCodecResult, equalTo(timeISO8601));
    }

    @Test
    public void testOTelProtoCodecConsistency() throws IOException, DecoderException {
        final ExportTraceServiceRequest request = buildExportTraceServiceRequestFromJsonFile(TEST_REQUEST_TRACE_JSON_FILE);
        final List<Span> spansFirstDec = decoderUnderTest.parseExportTraceServiceRequest(request, Instant.now());
        final List<ResourceSpans> resourceSpansList = new ArrayList<>();
        for (final Span span : spansFirstDec) {
            resourceSpansList.add(encoderUnderTest.convertToResourceSpans(span));
        }
        final List<Span> spansSecondDec = resourceSpansList.stream()
                .flatMap(rs -> decoderUnderTest.parseResourceSpans(rs, Instant.now()).stream()).collect(Collectors.toList());
        assertThat(spansFirstDec.size(), equalTo(spansSecondDec.size()));
        for (int i = 0; i < spansFirstDec.size(); i++) {
            assertThat(spansFirstDec.get(i).toJsonString(), equalTo(spansSecondDec.get(i).toJsonString()));
        }
    }

    @Test
    void getValueAsDouble() {
        Assertions.assertNull(OTelProtoStandardCodec.getValueAsDouble(NumberDataPoint.newBuilder().build()));
    }

    @Test
    public void testCreateBucketsEmpty() {
        MatcherAssert.assertThat(OTelProtoStandardCodec.createBuckets(new ArrayList<>(), new ArrayList<>()).size(), Matchers.equalTo(0));
    }

    @Test
    public void testCreateBuckets() {
        List<Long> bucketsCountList = List.of(1L, 2L, 3L, 4L);
        List<Double> explicitBOundsList = List.of(5D, 10D, 25D);
        List<Bucket> buckets = OTelProtoStandardCodec.createBuckets(bucketsCountList, explicitBOundsList);
        MatcherAssert.assertThat(buckets.size(), Matchers.equalTo(4));
        Bucket b1 = buckets.get(0);
        MatcherAssert.assertThat(b1.getCount(), Matchers.equalTo(1L));
        MatcherAssert.assertThat(b1.getMin(), Matchers.equalTo((double) -Float.MAX_VALUE));
        MatcherAssert.assertThat(b1.getMax(), Matchers.equalTo(5D));

        Bucket b2 = buckets.get(1);
        MatcherAssert.assertThat(b2.getCount(), Matchers.equalTo(2L));
        MatcherAssert.assertThat(b2.getMin(), Matchers.equalTo(5D));
        MatcherAssert.assertThat(b2.getMax(), Matchers.equalTo(10D));

        Bucket b3 = buckets.get(2);
        MatcherAssert.assertThat(b3.getCount(), Matchers.equalTo(3L));
        MatcherAssert.assertThat(b3.getMin(), Matchers.equalTo(10D));
        MatcherAssert.assertThat(b3.getMax(), Matchers.equalTo(25D));

        Bucket b4 = buckets.get(3);
        MatcherAssert.assertThat(b4.getCount(), Matchers.equalTo(4L));
        MatcherAssert.assertThat(b4.getMin(), Matchers.equalTo(25D));
        MatcherAssert.assertThat(b4.getMax(), Matchers.equalTo((double) Float.MAX_VALUE));
    }

    @Test
    public void testCreateBuckets_illegal_argument() {
        List<Long> bucketsCountList = List.of(1L, 2L, 3L, 4L);
        List<Double> boundsList = Collections.emptyList();
        Assertions.assertThrows(IllegalArgumentException.class, () -> OTelProtoStandardCodec.createBuckets(bucketsCountList, boundsList));
    }

    @Test
    public void testConvertAnyValueBool() {
        Object o = OTelProtoStandardCodec.convertAnyValue(AnyValue.newBuilder().setBoolValue(true).build());
        MatcherAssert.assertThat(o instanceof Boolean, Matchers.equalTo(true));
        MatcherAssert.assertThat(((boolean) o), Matchers.equalTo(true));
    }

    @Test
    public void testUnsupportedTypeToAnyValue() {
        Assertions.assertThrows(RuntimeException.class,
                () -> OTelProtoStandardCodec.convertAnyValue(AnyValue.newBuilder().setBytesValue(ByteString.EMPTY).build()));
    }

    @Test
    void convertExemplars() {
        Exemplar e1 = Exemplar.newBuilder()
                .addFilteredAttributes(KeyValue.newBuilder()
                        .setKey("key")
                        .setValue(AnyValue.newBuilder().setBoolValue(true).build()).build())
                .setAsDouble(3)
                .setSpanId(ByteString.copyFrom(getRandomBytes(8)))
                .setTimeUnixNano(TIME)
                .setTraceId(ByteString.copyFrom(getRandomBytes(8)))
                .build();


        Exemplar e2 = Exemplar.newBuilder()
                .addFilteredAttributes(KeyValue.newBuilder()
                        .setKey("key2")
                        .setValue(AnyValue.newBuilder()
                                .setArrayValue(ArrayValue.newBuilder().addValues(AnyValue.newBuilder().setStringValue("test").build()).build())
                                .build()).build())
                .setAsInt(42)
                .setSpanId(ByteString.copyFrom(getRandomBytes(8)))
                .setTimeUnixNano(TIME)
                .setTraceId(ByteString.copyFrom(getRandomBytes(8)))
                .build();

        List<io.opentelemetry.proto.metrics.v1.Exemplar> exemplars = Arrays.asList(e1, e2);
        List<org.opensearch.dataprepper.model.metric.Exemplar> convertedExemplars = OTelProtoStandardCodec.convertExemplars(exemplars);
        MatcherAssert.assertThat(convertedExemplars.size(), Matchers.equalTo(2));

        org.opensearch.dataprepper.model.metric.Exemplar conv1 = convertedExemplars.get(0);
        MatcherAssert.assertThat(conv1.getSpanId(), Matchers.equalTo(Hex.encodeHexString(e1.getSpanId().toByteArray())));
        MatcherAssert.assertThat(conv1.getTime(), Matchers.equalTo("2020-05-24T14:01:00Z"));
        MatcherAssert.assertThat(conv1.getTraceId(), Matchers.equalTo(Hex.encodeHexString(e1.getTraceId().toByteArray())));
        MatcherAssert.assertThat(conv1.getValue(), Matchers.equalTo(3.0));
        org.assertj.core.api.Assertions.assertThat(conv1.getAttributes()).contains(entry("key", true));

        org.opensearch.dataprepper.model.metric.Exemplar conv2 = convertedExemplars.get(1);
        MatcherAssert.assertThat(conv2.getSpanId(), Matchers.equalTo(Hex.encodeHexString(e2.getSpanId().toByteArray())));
        MatcherAssert.assertThat(conv2.getTime(), Matchers.equalTo("2020-05-24T14:01:00Z"));
        MatcherAssert.assertThat(conv2.getTraceId(), Matchers.equalTo(Hex.encodeHexString(e2.getTraceId().toByteArray())));
        MatcherAssert.assertThat(conv2.getValue(), Matchers.equalTo(42.0));
        org.assertj.core.api.Assertions.assertThat(conv2.getAttributes()).contains(entry("key2", List.of("test")));

    }


    /**
     * See: <a href="https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/datamodel.md#exponential-buckets">The example table with scale 3</a>
     */
    @Test
    public void testExponentialHistogram() {
        List<Bucket> b = OTelProtoStandardCodec.createExponentialBuckets(
                ExponentialHistogramDataPoint.Buckets.newBuilder()
                        .addBucketCounts(4)
                        .addBucketCounts(2)
                        .addBucketCounts(3)
                        .addBucketCounts(2)
                        .addBucketCounts(1)
                        .addBucketCounts(4)
                        .addBucketCounts(6)
                        .addBucketCounts(4)
                        .setOffset(0)
                        .build(), 3);

        MatcherAssert.assertThat(b.size(), Matchers.equalTo(8));

        Bucket b1 = b.get(0);
        MatcherAssert.assertThat(b1.getCount(), Matchers.equalTo(4L));
        MatcherAssert.assertThat(b1.getMin(), Matchers.equalTo(1D));
        MatcherAssert.assertThat(b1.getMax(), Matchers.closeTo(1.09051, MAX_ERROR));

        Bucket b2 = b.get(1);
        MatcherAssert.assertThat(b2.getCount(), Matchers.equalTo(2L));
        MatcherAssert.assertThat(b2.getMin(), Matchers.closeTo(1.09051, MAX_ERROR));
        MatcherAssert.assertThat(b2.getMax(), Matchers.closeTo(1.18921, MAX_ERROR));

        Bucket b3 = b.get(2);
        MatcherAssert.assertThat(b3.getCount(), Matchers.equalTo(3L));
        MatcherAssert.assertThat(b3.getMin(), Matchers.closeTo(1.18921, MAX_ERROR));
        MatcherAssert.assertThat(b3.getMax(), Matchers.closeTo(1.29684, MAX_ERROR));

        Bucket b4 = b.get(3);
        MatcherAssert.assertThat(b4.getCount(), Matchers.equalTo(2L));
        MatcherAssert.assertThat(b4.getMin(), Matchers.closeTo(1.29684, MAX_ERROR));
        MatcherAssert.assertThat(b4.getMax(), Matchers.closeTo(1.41421, MAX_ERROR));

        Bucket b5 = b.get(4);
        MatcherAssert.assertThat(b5.getCount(), Matchers.equalTo(1L));
        MatcherAssert.assertThat(b5.getMin(), Matchers.closeTo(1.41421, MAX_ERROR));
        MatcherAssert.assertThat(b5.getMax(), Matchers.closeTo(1.54221, MAX_ERROR));

        Bucket b6 = b.get(5);
        MatcherAssert.assertThat(b6.getCount(), Matchers.equalTo(4L));
        MatcherAssert.assertThat(b6.getMin(), Matchers.closeTo(1.54221, MAX_ERROR));
        MatcherAssert.assertThat(b6.getMax(), Matchers.closeTo(1.68179, MAX_ERROR));

        Bucket b7 = b.get(6);
        MatcherAssert.assertThat(b7.getCount(), Matchers.equalTo(6L));
        MatcherAssert.assertThat(b7.getMin(), Matchers.closeTo(1.68179, MAX_ERROR));
        MatcherAssert.assertThat(b7.getMax(), Matchers.closeTo(1.83401, MAX_ERROR));

        Bucket b8 = b.get(7);
        MatcherAssert.assertThat(b8.getCount(), Matchers.equalTo(4L));
        MatcherAssert.assertThat(b8.getMin(), Matchers.closeTo(1.83401, MAX_ERROR));
        MatcherAssert.assertThat(b8.getMax(), Matchers.closeTo(2, MAX_ERROR));
    }

    @Test
    public void testExponentialHistogramWithOffset() {
        List<Bucket> b = OTelProtoStandardCodec.createExponentialBuckets(
                ExponentialHistogramDataPoint.Buckets.newBuilder()
                        .addBucketCounts(4)
                        .addBucketCounts(2)
                        .addBucketCounts(3)
                        .addBucketCounts(2)
                        .addBucketCounts(1)
                        .addBucketCounts(4)
                        .setOffset(2)
                        .build(), 3);

        MatcherAssert.assertThat(b.size(), Matchers.equalTo(6));

        Bucket b1 = b.get(0);
        MatcherAssert.assertThat(b1.getCount(), Matchers.equalTo(4L));
        MatcherAssert.assertThat(b1.getMin(), Matchers.closeTo(1.18920, MAX_ERROR));
        MatcherAssert.assertThat(b1.getMax(), Matchers.closeTo(1.29684, MAX_ERROR));

        Bucket b2 = b.get(1);
        MatcherAssert.assertThat(b2.getCount(), Matchers.equalTo(2L));
        MatcherAssert.assertThat(b2.getMin(), Matchers.closeTo(1.29684, MAX_ERROR));
        MatcherAssert.assertThat(b2.getMax(), Matchers.closeTo(1.41421, MAX_ERROR));

        Bucket b3 = b.get(2);
        MatcherAssert.assertThat(b3.getCount(), Matchers.equalTo(3L));
        MatcherAssert.assertThat(b3.getMin(), Matchers.closeTo(1.41421, MAX_ERROR));
        MatcherAssert.assertThat(b3.getMax(), Matchers.closeTo(1.54221, MAX_ERROR));

        Bucket b4 = b.get(3);
        MatcherAssert.assertThat(b4.getCount(), Matchers.equalTo(2L));
        MatcherAssert.assertThat(b4.getMin(), Matchers.closeTo(1.54221, MAX_ERROR));
        MatcherAssert.assertThat(b4.getMax(), Matchers.closeTo(1.68179, MAX_ERROR));

        Bucket b5 = b.get(4);
        MatcherAssert.assertThat(b5.getCount(), Matchers.equalTo(1L));
        MatcherAssert.assertThat(b5.getMin(), Matchers.closeTo(1.68179, MAX_ERROR));
        MatcherAssert.assertThat(b5.getMax(), Matchers.closeTo(1.83401, MAX_ERROR));

        Bucket b6 = b.get(5);
        MatcherAssert.assertThat(b6.getCount(), Matchers.equalTo(4L));
        MatcherAssert.assertThat(b6.getMin(), Matchers.closeTo(1.83401, MAX_ERROR));
        MatcherAssert.assertThat(b6.getMax(), Matchers.closeTo(2, MAX_ERROR));
    }

    @Test
    public void testExponentialHistogramWithLargeOffset() {
        List<Bucket> b = OTelProtoStandardCodec.createExponentialBuckets(
                ExponentialHistogramDataPoint.Buckets.newBuilder()
                        .addBucketCounts(4)
                        .addBucketCounts(2)
                        .addBucketCounts(3)
                        .addBucketCounts(2)
                        .setOffset(20)
                        .build(), 2);

        MatcherAssert.assertThat(b.size(), Matchers.equalTo(4));

        Bucket b1 = b.get(0);
        MatcherAssert.assertThat(b1.getCount(), Matchers.equalTo(4L));
        MatcherAssert.assertThat(b1.getMin(), Matchers.closeTo(32.0, MAX_ERROR));
        MatcherAssert.assertThat(b1.getMax(), Matchers.closeTo(38.05462, MAX_ERROR));

        Bucket b2 = b.get(1);
        MatcherAssert.assertThat(b2.getCount(), Matchers.equalTo(2L));
        MatcherAssert.assertThat(b2.getMin(), Matchers.closeTo(38.05462, MAX_ERROR));
        MatcherAssert.assertThat(b2.getMax(), Matchers.closeTo(45.254833, MAX_ERROR));

        Bucket b3 = b.get(2);
        MatcherAssert.assertThat(b3.getCount(), Matchers.equalTo(3L));
        MatcherAssert.assertThat(b3.getMin(), Matchers.closeTo(45.254833, MAX_ERROR));
        MatcherAssert.assertThat(b3.getMax(), Matchers.closeTo(53.81737, MAX_ERROR));

        Bucket b4 = b.get(3);
        MatcherAssert.assertThat(b4.getCount(), Matchers.equalTo(2L));
        MatcherAssert.assertThat(b4.getMin(), Matchers.closeTo(53.81737, MAX_ERROR));
        MatcherAssert.assertThat(b4.getMax(), Matchers.closeTo(63.99999, MAX_ERROR));
    }

    @Test
    public void testExponentialHistogramWithNegativeOffset() {
        List<Bucket> b = OTelProtoStandardCodec.createExponentialBuckets(
                ExponentialHistogramDataPoint.Buckets.newBuilder()
                        .addBucketCounts(4)
                        .addBucketCounts(2)
                        .addBucketCounts(3)
                        .addBucketCounts(2)
                        .setOffset(-5)
                        .build(), 2);

        MatcherAssert.assertThat(b.size(), Matchers.equalTo(4));

        Bucket b1 = b.get(0);
        MatcherAssert.assertThat(b1.getCount(), Matchers.equalTo(4L));
        MatcherAssert.assertThat(b1.getMin(), Matchers.closeTo(0.42044820762685736, MAX_ERROR));
        MatcherAssert.assertThat(b1.getMax(), Matchers.closeTo(0.35355339059327384, MAX_ERROR));

        Bucket b2 = b.get(1);
        MatcherAssert.assertThat(b2.getCount(), Matchers.equalTo(2L));
        MatcherAssert.assertThat(b2.getMin(), Matchers.closeTo(0.35355339059327384, MAX_ERROR));
        MatcherAssert.assertThat(b2.getMax(), Matchers.closeTo(0.2973017787506803, MAX_ERROR));

        Bucket b3 = b.get(2);
        MatcherAssert.assertThat(b3.getCount(), Matchers.equalTo(3L));
        MatcherAssert.assertThat(b3.getMin(), Matchers.closeTo(0.2973017787506803, MAX_ERROR));
        MatcherAssert.assertThat(b3.getMax(), Matchers.closeTo(0.2500000000, MAX_ERROR));

        Bucket b4 = b.get(3);
        MatcherAssert.assertThat(b4.getCount(), Matchers.equalTo(2L));
        MatcherAssert.assertThat(b4.getMin(), Matchers.closeTo(0.2500000000, MAX_ERROR));
        MatcherAssert.assertThat(b4.getMax(), Matchers.closeTo(0.2102241038134287, MAX_ERROR));
    }

    @Test
    public void testExponentialHistogramWithNegativeScale() {
        List<Bucket> b = OTelProtoStandardCodec.createExponentialBuckets(
                ExponentialHistogramDataPoint.Buckets.newBuilder()
                        .addBucketCounts(4)
                        .addBucketCounts(2)
                        .addBucketCounts(3)
                        .addBucketCounts(2)
                        .setOffset(0)
                        .build(), -2);

        MatcherAssert.assertThat(b.size(), Matchers.equalTo(4));

        Bucket b1 = b.get(0);
        MatcherAssert.assertThat(b1.getCount(), Matchers.equalTo(4L));
        MatcherAssert.assertThat(b1.getMin(), Matchers.closeTo(1, MAX_ERROR));
        MatcherAssert.assertThat(b1.getMax(), Matchers.closeTo(16, MAX_ERROR));

        Bucket b2 = b.get(1);
        MatcherAssert.assertThat(b2.getCount(), Matchers.equalTo(2L));
        MatcherAssert.assertThat(b2.getMin(), Matchers.closeTo(16, MAX_ERROR));
        MatcherAssert.assertThat(b2.getMax(), Matchers.closeTo(256, MAX_ERROR));

        Bucket b3 = b.get(2);
        MatcherAssert.assertThat(b3.getCount(), Matchers.equalTo(3L));
        MatcherAssert.assertThat(b3.getMin(), Matchers.closeTo(256, MAX_ERROR));
        MatcherAssert.assertThat(b3.getMax(), Matchers.closeTo(4096, MAX_ERROR));

        Bucket b4 = b.get(3);
        MatcherAssert.assertThat(b4.getCount(), Matchers.equalTo(2L));
        MatcherAssert.assertThat(b4.getMin(), Matchers.closeTo(4096, MAX_ERROR));
        MatcherAssert.assertThat(b4.getMax(), Matchers.closeTo(65536, MAX_ERROR));
    }

    @Test
    public void testExponentialHistogramWithMaxOffsetOutOfRange() {
        List<Bucket> b = OTelProtoStandardCodec.createExponentialBuckets(
                ExponentialHistogramDataPoint.Buckets.newBuilder()
                        .addBucketCounts(4)
                        .addBucketCounts(2)
                        .addBucketCounts(3)
                        .addBucketCounts(2)
                        .setOffset(1025)
                        .build(), -2);

        MatcherAssert.assertThat(b.size(), Matchers.equalTo(0));
    }

    @Test
    public void testExponentialHistogramWithMaxNegativeOffsetOutOfRange() {
        List<Bucket> b = OTelProtoStandardCodec.createExponentialBuckets(
                ExponentialHistogramDataPoint.Buckets.newBuilder()
                        .addBucketCounts(4)
                        .addBucketCounts(2)
                        .addBucketCounts(3)
                        .addBucketCounts(2)
                        .setOffset(-1025)
                        .build(), -2);

        MatcherAssert.assertThat(b.size(), Matchers.equalTo(0));
    }

    @Test
    public void testBoundsKeyEquals() {
        OTelProtoStandardCodec.BoundsKey k1 = new OTelProtoStandardCodec.BoundsKey(2, OTelProtoStandardCodec.BoundsKey.Sign.POSITIVE);
        OTelProtoStandardCodec.BoundsKey k2 = new OTelProtoStandardCodec.BoundsKey(2, OTelProtoStandardCodec.BoundsKey.Sign.POSITIVE);
        assertEquals(k1, k2);
    }

    @Test
    public void testBoundsKeyNotEqualsScale() {
        OTelProtoStandardCodec.BoundsKey k1 = new OTelProtoStandardCodec.BoundsKey(2, OTelProtoStandardCodec.BoundsKey.Sign.POSITIVE);
        OTelProtoStandardCodec.BoundsKey k2 = new OTelProtoStandardCodec.BoundsKey(-2, OTelProtoStandardCodec.BoundsKey.Sign.POSITIVE);
        assertNotEquals(k1, k2);
    }

    @Test
    public void testBoundsKeyNotEqualsSign() {
        OTelProtoStandardCodec.BoundsKey k1 = new OTelProtoStandardCodec.BoundsKey(2, OTelProtoStandardCodec.BoundsKey.Sign.POSITIVE);
        OTelProtoStandardCodec.BoundsKey k2 = new OTelProtoStandardCodec.BoundsKey(2, OTelProtoStandardCodec.BoundsKey.Sign.NEGATIVE);
        assertNotEquals(k1, k2);
    }

}
