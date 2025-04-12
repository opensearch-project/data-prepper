/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.xrayotlp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.codec.OutputCodec;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.DefaultTraceGroupFields;
import org.opensearch.dataprepper.model.trace.JacksonSpan;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.sink.xrayotlp.http.XRayOtlpHttpSender;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class XRayOTLPSinkTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String TEST_SPAN_EVENT_JSON_FILE = "test-span-event.json";

    private OutputCodec mockCodec;
    private XRayOtlpHttpSender mockSender;
    private XRayOTLPSink sink;

    @BeforeEach
    void setUp() {
        mockCodec = mock(OutputCodec.class);
        mockSender = mock(XRayOtlpHttpSender.class);
        sink = new XRayOTLPSink(mockCodec, mockSender);
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
    void testOutput_sendsDataToXRay_onSuccessResponse() throws Exception {
        final Span span = buildSpanFromTestFile(TEST_SPAN_EVENT_JSON_FILE, "bad-trace-id");
        Record<Span> record = new Record<>(span);

        doAnswer(invocation -> {
            OutputStream out = invocation.getArgument(1);
            out.write("dummy-otlp-payload".getBytes());
            return null;
        }).when(mockCodec).writeEvent(eq(span), any());

        doNothing().when(mockSender).send(eq("dummy-otlp-payload".getBytes()));

        sink.output(Collections.singletonList(record));

        verify(mockCodec).writeEvent(eq(span), any());
        verify(mockSender).send(eq("dummy-otlp-payload".getBytes()));
    }

    @Test
    void testOutput_handlesException_gracefully() throws Exception {
        Span span = mock(Span.class);
        Record<Span> record = new Record<>(span);

        doThrow(new RuntimeException("codec error")).when(mockCodec).writeEvent(eq(span), any());

        sink.output(Collections.singletonList(record));

        verify(mockCodec).writeEvent(eq(span), any());
        verifyNoInteractions(mockSender);
    }

    @Test
    void testOutput_senderThrowsException_isHandledGracefully() throws Exception {
        final Span span = buildSpanFromTestFile(TEST_SPAN_EVENT_JSON_FILE, "bad-trace-id");
        Record<Span> record = new Record<>(span);

        doAnswer(invocation -> {
            OutputStream out = invocation.getArgument(1);
            out.write("dummy-otlp-payload".getBytes());
            return null;
        }).when(mockCodec).writeEvent(eq(span), any());

        doThrow(new RuntimeException("Send failed")).when(mockSender).send(any());

        assertDoesNotThrow(() -> sink.output(Collections.singletonList(record)));

        verify(mockCodec).writeEvent(eq(span), any());
        verify(mockSender).send(any());
    }

    @Test
    void testOutput_withNullRecordList_doesNothing() {
        assertDoesNotThrow(() -> sink.output(null));
        verifyNoInteractions(mockCodec, mockSender);
    }

    @Test
    void testOutput_withEmptyRecordList_doesNothing() {
        assertDoesNotThrow(() -> sink.output(Collections.emptyList()));
        verifyNoInteractions(mockCodec, mockSender);
    }

    @Test
    void testInitialize_doesNotThrow() {
        assertDoesNotThrow(() -> sink.initialize());
    }

    @Test
    void testIsReady_returnsTrue() {
        assertTrue(sink.isReady());
    }

    @Test
    void testShutdown_doesNotThrow() {
        assertDoesNotThrow(() -> sink.shutdown());
    }
}
