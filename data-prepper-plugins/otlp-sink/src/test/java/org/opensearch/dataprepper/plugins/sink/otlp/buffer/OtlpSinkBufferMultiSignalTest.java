/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugins.sink.otlp.buffer;

import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.sink.otlp.OtlpSignalHandler;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.http.OtlpHttpSender;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;
import software.amazon.awssdk.regions.Region;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for OtlpSinkBuffer with signal-specific handlers.
 */
class OtlpSinkBufferMultiSignalTest {

    private OtlpSinkConfig config;
    private OtlpSinkMetrics metrics;
    private OtlpSignalHandler handler;
    private OtlpHttpSender sender;
    private OtlpSinkBuffer buffer;

    @BeforeEach
    void setUp() {
        config = mock(OtlpSinkConfig.class);
        when(config.getEndpoint()).thenReturn("https://localhost/v1/traces");
        when(config.getMaxEvents()).thenReturn(10);
        when(config.getMaxRetries()).thenReturn(2);
        when(config.getMaxBatchSize()).thenReturn(1_000_000L);
        when(config.getFlushTimeoutMillis()).thenReturn(100L);
        when(config.getAwsRegion()).thenReturn(Region.of("us-west-2"));

        metrics = mock(OtlpSinkMetrics.class);
        handler = mock(OtlpSignalHandler.class);
        sender = mock(OtlpHttpSender.class);

        buffer = new OtlpSinkBuffer(config, metrics, handler, sender);
    }

    @AfterEach
    void tearDown() {
        if (buffer != null) {
            buffer.stop();
        }
    }

    @Test
    void testProcessTraceEvents() throws Exception {
        final ResourceSpans resourceSpans = ResourceSpans.getDefaultInstance();
        when(handler.encodeEvent(any(Event.class))).thenReturn(resourceSpans);
        when(handler.getSerializedSize(any())).thenReturn(100L);

        buffer.start();

        final Record<Event> record1 = createMockSpanRecord();
        final Record<Event> record2 = createMockSpanRecord();

        buffer.add(record1);
        buffer.add(record2);

        await().atMost(2, SECONDS).untilAsserted(() -> 
            verify(sender).send(any(), any(OtlpSignalHandler.class))
        );

        buffer.stop();
    }

    @Test
    void testProcessMetricEvents() throws Exception {
        final ResourceMetrics resourceMetrics = ResourceMetrics.getDefaultInstance();
        when(handler.encodeEvent(any(Event.class))).thenReturn(resourceMetrics);
        when(handler.getSerializedSize(any())).thenReturn(100L);

        buffer.start();

        final Record<Event> record1 = createMockMetricRecord();
        final Record<Event> record2 = createMockMetricRecord();

        buffer.add(record1);
        buffer.add(record2);

        await().atMost(2, SECONDS).untilAsserted(() -> 
            verify(sender).send(any(), any(OtlpSignalHandler.class))
        );

        buffer.stop();
    }

    @Test
    void testProcessLogEvents() throws Exception {
        final ResourceLogs resourceLogs = ResourceLogs.getDefaultInstance();
        when(handler.encodeEvent(any(Event.class))).thenReturn(resourceLogs);
        when(handler.getSerializedSize(any())).thenReturn(100L);

        buffer.start();

        final Record<Event> record1 = createMockLogRecord();
        final Record<Event> record2 = createMockLogRecord();

        buffer.add(record1);
        buffer.add(record2);

        await().atMost(2, SECONDS).untilAsserted(() -> 
            verify(sender).send(any(), any(OtlpSignalHandler.class))
        );

        buffer.stop();
    }

    @Test
    void testBatchFlushByCount() throws Exception {
        when(config.getMaxEvents()).thenReturn(2);
        when(handler.encodeEvent(any(Event.class))).thenReturn(ResourceSpans.getDefaultInstance());
        when(handler.getSerializedSize(any())).thenReturn(100L);

        buffer = new OtlpSinkBuffer(config, metrics, handler, sender);
        buffer.start();

        buffer.add(createMockSpanRecord());
        buffer.add(createMockSpanRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> 
            verify(sender, atLeast(1)).send(any(), any(OtlpSignalHandler.class))
        );

        buffer.stop();
    }

    private Record<Event> createMockSpanRecord() {
        final Span span = mock(Span.class);
        final EventHandle eventHandle = mock(EventHandle.class);
        when(span.getEventHandle()).thenReturn(eventHandle);
        return new Record<>(span);
    }

    private Record<Event> createMockMetricRecord() {
        final Metric metric = mock(Metric.class);
        final EventHandle eventHandle = mock(EventHandle.class);
        when(metric.getEventHandle()).thenReturn(eventHandle);
        return new Record<>(metric);
    }

    private Record<Event> createMockLogRecord() {
        final Log log = mock(Log.class);
        final EventHandle eventHandle = mock(EventHandle.class);
        when(log.getEventHandle()).thenReturn(eventHandle);
        return new Record<>(log);
    }
}
