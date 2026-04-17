/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.codec;

import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.common.v1.KeyValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventMetadata;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OtlpLogEncoderTest {

    private OtlpLogEncoder encoder;

    @BeforeEach
    void setUp() {
        encoder = new OtlpLogEncoder();
    }

    @Test
    void testEncode_basicEvent() {
        final Event event = mockEvent(Map.of("message", "hello"), "{\"message\":\"hello\"}", null);

        final ResourceLogs result = encoder.encode(event);

        assertNotNull(result);
        assertEquals(1, result.getScopeLogsCount());
        assertEquals(1, result.getScopeLogs(0).getLogRecordsCount());

        final LogRecord logRecord = result.getScopeLogs(0).getLogRecords(0);
        assertEquals("{\"message\":\"hello\"}", logRecord.getBody().getStringValue());
        assertTrue(logRecord.getTimeUnixNano() > 0);
        assertTrue(logRecord.getObservedTimeUnixNano() > 0);
    }

    @Test
    void testEncode_setsTimeFromMetadata() {
        final Instant eventTime = Instant.parse("2026-01-15T10:30:00Z");
        final Event event = mockEvent(Map.of("msg", "test"), "{\"msg\":\"test\"}", eventTime);

        final ResourceLogs result = encoder.encode(event);
        final LogRecord logRecord = result.getScopeLogs(0).getLogRecords(0);

        final long expectedNanos = eventTime.getEpochSecond() * 1_000_000_000L + eventTime.getNano();
        assertEquals(expectedNanos, logRecord.getTimeUnixNano());
    }

    @Test
    void testEncode_usesCurrentTimeWhenNoMetadata() {
        final Event event = mockEvent(Map.of("msg", "test"), "{\"msg\":\"test\"}", null);
        when(event.getMetadata()).thenReturn(null);

        final long before = System.currentTimeMillis() * 1_000_000L;
        final ResourceLogs result = encoder.encode(event);
        final long after = System.currentTimeMillis() * 1_000_000L;

        final LogRecord logRecord = result.getScopeLogs(0).getLogRecords(0);
        assertTrue(logRecord.getTimeUnixNano() >= before);
        assertTrue(logRecord.getTimeUnixNano() <= after);
    }

    @Test
    void testEncode_typedAttributes() {
        final Map<String, Object> data = new LinkedHashMap<>();
        data.put("str", "value");
        data.put("bool", true);
        data.put("intVal", 42);
        data.put("longVal", 100L);
        data.put("floatVal", 1.5f);
        data.put("doubleVal", 3.14);
        data.put("nullVal", null);

        final Event event = mockEvent(data, "{}", null);

        final ResourceLogs result = encoder.encode(event);
        final LogRecord logRecord = result.getScopeLogs(0).getLogRecords(0);

        assertEquals(7, logRecord.getAttributesCount());

        final Map<String, KeyValue> attrs = new LinkedHashMap<>();
        for (final KeyValue kv : logRecord.getAttributesList()) {
            attrs.put(kv.getKey(), kv);
        }

        assertEquals("value", attrs.get("str").getValue().getStringValue());
        assertTrue(attrs.get("bool").getValue().getBoolValue());
        assertEquals(42, attrs.get("intVal").getValue().getIntValue());
        assertEquals(100L, attrs.get("longVal").getValue().getIntValue());
        assertEquals(3.14, attrs.get("doubleVal").getValue().getDoubleValue(), 0.001);
        assertTrue(attrs.get("floatVal").getValue().getDoubleValue() > 1.0);
    }

    @Test
    void testEncode_unsupportedTypeUsesToString() {
        final Map<String, Object> data = new LinkedHashMap<>();
        data.put("list", java.util.List.of("a", "b"));

        final Event event = mockEvent(data, "{}", null);

        final ResourceLogs result = encoder.encode(event);
        final LogRecord logRecord = result.getScopeLogs(0).getLogRecords(0);

        assertEquals("[a, b]", logRecord.getAttributes(0).getValue().getStringValue());
    }

    @Test
    void testEncode_scopeName() {
        final Event event = mockEvent(Map.of(), "{}", null);

        final ResourceLogs result = encoder.encode(event);

        assertEquals("data-prepper-otlp-sink", result.getScopeLogs(0).getScope().getName());
    }

    @Test
    void testEncode_bodyIsFullJson() {
        final String json = "{\"message\":\"hello\",\"count\":5}";
        final Event event = mockEvent(Map.of("message", "hello", "count", 5), json, null);

        final ResourceLogs result = encoder.encode(event);
        final LogRecord logRecord = result.getScopeLogs(0).getLogRecords(0);

        assertFalse(logRecord.getBody().getStringValue().isEmpty());
    }

    private Event mockEvent(final Map<String, Object> data, final String json, final Instant timeReceived) {
        final Event event = mock(Event.class);
        when(event.toMap()).thenReturn(data);
        when(event.toJsonString()).thenReturn(json);

        final EventMetadata metadata = mock(EventMetadata.class);
        when(metadata.getTimeReceived()).thenReturn(timeReceived);
        when(event.getMetadata()).thenReturn(metadata);

        return event;
    }
}
