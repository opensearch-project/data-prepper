/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.otel.codec;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.sink.OutputCodecContext;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Map;
import java.util.Objects;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OtlpTraceOutputCodecTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TEST_SPAN_EVENT_JSON_FILE = "test-span-event.json";

    private OtlpTraceOutputCodec codec;

    @BeforeEach
    void setup() {
        codec = new OtlpTraceOutputCodec();
    }

    private Span buildSpanFromTestFile(String fileName, String traceIdOverride) {
        try (InputStream inputStream = Objects.requireNonNull(
                getClass().getClassLoader().getResourceAsStream(fileName))) {

            final Map<String, Object> spanMap = OBJECT_MAPPER.readValue(inputStream, new TypeReference<>() {});
            final JacksonSpan.Builder builder = JacksonSpan.builder()
                    .withTraceId(traceIdOverride != null ? traceIdOverride : (String) spanMap.get("traceId"))
                    .withSpanId((String) spanMap.get("spanId"))
                    .withParentSpanId((String) spanMap.get("parentSpanId"))
                    .withTraceState((String) spanMap.get("traceState"))
                    .withName((String) spanMap.get("name"))
                    .withKind((String) spanMap.get("kind"))
                    .withDurationInNanos(((Number) spanMap.get("durationInNanos")).longValue())
                    .withStartTime((String) spanMap.get("startTime"))
                    .withEndTime((String) spanMap.get("endTime"))
                    .withTraceGroup((String) spanMap.get("traceGroup"));

            final Map<String, Object> traceGroupFieldsMap = (Map<String, Object>) spanMap.get("traceGroupFields");
            if (traceGroupFieldsMap != null) {
                builder.withTraceGroupFields(DefaultTraceGroupFields.builder()
                        .withStatusCode((Integer) traceGroupFieldsMap.getOrDefault("statusCode", 0))
                        .withEndTime((String) spanMap.get("endTime"))
                        .withDurationInNanos(((Number) spanMap.get("durationInNanos")).longValue())
                        .build());
            }

            return builder.build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to load span from file", e);
        }
    }

    @Test
    void testWriteEvent_withValidSpanFromTestFile_writesSuccessfully() throws Exception {
        final Span span = buildSpanFromTestFile(TEST_SPAN_EVENT_JSON_FILE, null);
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        codec.start(outputStream, span, new OutputCodecContext());
        codec.writeEvent(span, outputStream);
        codec.complete(outputStream);

        final byte[] bytes = outputStream.toByteArray();
        assertThat(bytes).isNotEmpty();

        final ExportTraceServiceRequest request = ExportTraceServiceRequest.parseFrom(bytes);
        assertThat(request.getResourceSpansCount()).isGreaterThan(0);
    }

    @Test
    void testWriteEvent_withBadTraceId_throwsException() throws Exception {
        final Span span = buildSpanFromTestFile(TEST_SPAN_EVENT_JSON_FILE,"bad-trace-id" );

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        assertThatThrownBy(() -> codec.writeEvent(span, outputStream))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void testWriteEvent_withNonSpanEvent_throwsException() {
        final JacksonEvent nonSpanEvent = JacksonEvent.builder()
                .withEventType("fake")
                .withData(Map.of("key", "value"))
                .build();
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        assertThatThrownBy(() -> codec.writeEvent(nonSpanEvent, outputStream))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("OtlpTraceOutputCodec only supports Span events");
    }

    @Test
    void testWriteEvent_withNullEvent_throwsNullPointerException() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        assertThatThrownBy(() -> codec.writeEvent(null, outputStream))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("event is marked non-null but is null");
    }

    @Test
    void testWriteEvent_withNullOutputStream_throwsNullPointerException() {
        final Span span = buildSpanFromTestFile(TEST_SPAN_EVENT_JSON_FILE, null);

        assertThatThrownBy(() -> codec.writeEvent(span, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessage("outputStream is marked non-null but is null");
    }
}
