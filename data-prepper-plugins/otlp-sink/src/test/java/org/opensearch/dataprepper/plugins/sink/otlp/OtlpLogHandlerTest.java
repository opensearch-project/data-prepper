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

import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsPartialSuccess;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import software.amazon.awssdk.utils.Pair;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OtlpLogHandlerTest {

    private OTelProtoStandardCodec.OTelProtoEncoder encoder;
    private OtlpLogHandler handler;

    @BeforeEach
    void setUp() {
        encoder = mock(OTelProtoStandardCodec.OTelProtoEncoder.class);
        handler = new OtlpLogHandler(encoder);
    }

    @Test
    void testEncodeEvent() throws Exception {
        final Log log = mock(Log.class);
        final ResourceLogs resourceLogs = ResourceLogs.getDefaultInstance();
        when(encoder.convertToResourceLogs(log)).thenReturn(resourceLogs);

        final Object result = handler.encodeEvent(log);

        assertEquals(resourceLogs, result);
    }

    @Test
    void testEncodeEvent_wrongType_throwsIllegalArgumentException() {
        final org.opensearch.dataprepper.model.event.Event wrongType = mock(org.opensearch.dataprepper.model.event.Event.class);
        assertThrows(IllegalArgumentException.class, () -> handler.encodeEvent(wrongType));
    }

    @Test
    void testGetSerializedSize() {
        final ResourceLogs resourceLogs = ResourceLogs.getDefaultInstance();

        final long size = handler.getSerializedSize(resourceLogs);

        assertEquals(resourceLogs.getSerializedSize(), size);
    }

    @Test
    void testBuildRequestPayload() {
        final ResourceLogs rl = ResourceLogs.getDefaultInstance();
        final EventHandle handle = mock(EventHandle.class);
        final List<Pair<ResourceLogs, EventHandle>> batch = List.of(Pair.of(rl, handle));

        final byte[] payload = handler.buildRequestPayload(batch);

        assertNotNull(payload);
        assertTrue(payload.length > 0);
    }

    @Test
    void testParsePartialSuccess_withPartialSuccess() throws Exception {
        final ExportLogsServiceResponse response = ExportLogsServiceResponse.newBuilder()
                .setPartialSuccess(ExportLogsPartialSuccess.newBuilder()
                        .setRejectedLogRecords(3)
                        .setErrorMessage("log error"))
                .build();

        final Pair<Long, String> result = handler.parsePartialSuccess(response.toByteArray());

        assertEquals(3L, result.left());
        assertEquals("log error", result.right());
    }

    @Test
    void testParsePartialSuccess_withoutPartialSuccess() throws Exception {
        final ExportLogsServiceResponse response = ExportLogsServiceResponse.getDefaultInstance();

        final Pair<Long, String> result = handler.parsePartialSuccess(response.toByteArray());

        assertEquals(0L, result.left());
        assertEquals("", result.right());
    }
}
