/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.otlp;

import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceResponse;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import software.amazon.awssdk.utils.Pair;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OtlpTraceHandlerTest {

    private OTelProtoStandardCodec.OTelProtoEncoder encoder;
    private OtlpTraceHandler handler;

    @BeforeEach
    void setUp() {
        encoder = mock(OTelProtoStandardCodec.OTelProtoEncoder.class);
        handler = new OtlpTraceHandler(encoder);
    }

    @Test
    void testEncodeEvent() throws Exception {
        final Span span = mock(Span.class);
        final ResourceSpans resourceSpans = ResourceSpans.getDefaultInstance();
        when(encoder.convertToResourceSpans(span)).thenReturn(resourceSpans);

        final Object result = handler.encodeEvent(span);

        assertEquals(resourceSpans, result);
    }

    @Test
    void testEncodeEvent_wrongType_throwsIllegalArgumentException() {
        final org.opensearch.dataprepper.model.event.Event wrongType = mock(org.opensearch.dataprepper.model.event.Event.class);
        assertThrows(IllegalArgumentException.class, () -> handler.encodeEvent(wrongType));
    }

    @Test
    void testGetSerializedSize() {
        final ResourceSpans resourceSpans = ResourceSpans.getDefaultInstance();

        final long size = handler.getSerializedSize(resourceSpans);

        assertEquals(resourceSpans.getSerializedSize(), size);
    }

    @Test
    void testBuildRequestPayload() {
        final ResourceSpans rs = ResourceSpans.getDefaultInstance();
        final EventHandle handle = mock(EventHandle.class);
        final List<Pair<ResourceSpans, EventHandle>> batch = List.of(Pair.of(rs, handle));

        final byte[] payload = handler.buildRequestPayload(batch);

        assertNotNull(payload);
        assertTrue(payload.length > 0);
    }

    @Test
    void testParsePartialSuccess_withPartialSuccess() throws Exception {
        final ExportTraceServiceResponse response = ExportTraceServiceResponse.newBuilder()
                .setPartialSuccess(ExportTraceServiceResponse.getDefaultInstance().getPartialSuccess().toBuilder()
                        .setRejectedSpans(5)
                        .setErrorMessage("some error"))
                .build();

        final Pair<Long, String> result = handler.parsePartialSuccess(response.toByteArray());

        assertEquals(5L, result.left());
        assertEquals("some error", result.right());
    }

    @Test
    void testParsePartialSuccess_withoutPartialSuccess() throws Exception {
        final ExportTraceServiceResponse response = ExportTraceServiceResponse.getDefaultInstance();

        final Pair<Long, String> result = handler.parsePartialSuccess(response.toByteArray());

        assertEquals(0L, result.left());
        assertEquals("", result.right());
    }
}
