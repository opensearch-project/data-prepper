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

import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceResponse;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsPartialSuccess;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import software.amazon.awssdk.utils.Pair;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OtlpMetricHandlerTest {

    private OTelProtoStandardCodec.OTelProtoEncoder encoder;
    private OtlpMetricHandler handler;

    @BeforeEach
    void setUp() {
        encoder = mock(OTelProtoStandardCodec.OTelProtoEncoder.class);
        handler = new OtlpMetricHandler(encoder);
    }

    @Test
    void testEncodeEvent() throws Exception {
        final Metric metric = mock(Metric.class);
        final ResourceMetrics resourceMetrics = ResourceMetrics.getDefaultInstance();
        when(encoder.convertToResourceMetrics(metric)).thenReturn(resourceMetrics);

        final Object result = handler.encodeEvent(metric);

        assertEquals(resourceMetrics, result);
    }

    @Test
    void testEncodeEvent_wrongType_throwsIllegalArgumentException() {
        final org.opensearch.dataprepper.model.event.Event wrongType = mock(org.opensearch.dataprepper.model.event.Event.class);
        assertThrows(IllegalArgumentException.class, () -> handler.encodeEvent(wrongType));
    }

    @Test
    void testGetSerializedSize() {
        final ResourceMetrics resourceMetrics = ResourceMetrics.getDefaultInstance();

        final long size = handler.getSerializedSize(resourceMetrics);

        assertEquals(resourceMetrics.getSerializedSize(), size);
    }

    @Test
    void testBuildRequestPayload() {
        final ResourceMetrics rm = ResourceMetrics.getDefaultInstance();
        final EventHandle handle = mock(EventHandle.class);
        final List<Pair<ResourceMetrics, EventHandle>> batch = List.of(Pair.of(rm, handle));

        final byte[] payload = handler.buildRequestPayload(batch);

        assertNotNull(payload);
        assertTrue(payload.length > 0);
    }

    @Test
    void testParsePartialSuccess_withPartialSuccess() throws Exception {
        final ExportMetricsServiceResponse response = ExportMetricsServiceResponse.newBuilder()
                .setPartialSuccess(ExportMetricsPartialSuccess.newBuilder()
                        .setRejectedDataPoints(7)
                        .setErrorMessage("metric error"))
                .build();

        final Pair<Long, String> result = handler.parsePartialSuccess(response.toByteArray());

        assertEquals(7L, result.left());
        assertEquals("metric error", result.right());
    }

    @Test
    void testParsePartialSuccess_withoutPartialSuccess() throws Exception {
        final ExportMetricsServiceResponse response = ExportMetricsServiceResponse.getDefaultInstance();

        final Pair<Long, String> result = handler.parsePartialSuccess(response.toByteArray());

        assertEquals(0L, result.left());
        assertEquals("", result.right());
    }
}
