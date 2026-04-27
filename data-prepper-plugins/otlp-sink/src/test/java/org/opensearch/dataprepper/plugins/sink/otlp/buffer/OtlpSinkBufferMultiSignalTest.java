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
import org.opensearch.dataprepper.plugins.sink.otlp.OtlpSignalType;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.http.OtlpHttpSender;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;
import software.amazon.awssdk.regions.Region;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
    private OtlpSignalHandler<?> handler;
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

        buffer = new OtlpSinkBuffer(config, metrics, handler, sender, OtlpSignalType.TRACE);
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
            verify(sender).send(any(), any(OtlpSignalHandler.class), any(OtlpSignalType.class))
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
            verify(sender).send(any(), any(OtlpSignalHandler.class), any(OtlpSignalType.class))
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
            verify(sender).send(any(), any(OtlpSignalHandler.class), any(OtlpSignalType.class))
        );

        buffer.stop();
    }

    @Test
    void testBatchFlushByCount() throws Exception {
        when(config.getMaxEvents()).thenReturn(2);
        when(handler.encodeEvent(any(Event.class))).thenReturn(ResourceSpans.getDefaultInstance());
        when(handler.getSerializedSize(any())).thenReturn(100L);

        buffer = new OtlpSinkBuffer(config, metrics, handler, sender, OtlpSignalType.TRACE);
        buffer.start();

        buffer.add(createMockSpanRecord());
        buffer.add(createMockSpanRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> 
            verify(sender, atLeast(1)).send(any(), any(OtlpSignalHandler.class), any(OtlpSignalType.class))
        );

        buffer.stop();
    }

    @Test
    void testEncodeFailure_incrementsErrorMetrics() throws Exception {
        when(handler.encodeEvent(any(Event.class))).thenThrow(new RuntimeException("encode failed"));

        buffer.start();

        buffer.add(createMockSpanRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(metrics, atLeast(1)).incrementFailedRecordsCount(1);
            verify(metrics, atLeast(1)).incrementErrorsCount();
        });

        buffer.stop();
    }

    @Test
    void testEncodeReturnsNull_doesNotAddToBatch() throws Exception {
        when(handler.encodeEvent(any(Event.class))).thenReturn(null);

        buffer.start();

        buffer.add(createMockSpanRecord());

        // Give time for processing
        Thread.sleep(300);

        // Sender should never be called since nothing was added to batch
        verify(sender, org.mockito.Mockito.never()).send(any(), any(OtlpSignalHandler.class), any(OtlpSignalType.class));

        buffer.stop();
    }

    @Test
    void testFlushBySize_triggersWhenBatchSizeExceedsMax() throws Exception {
        when(config.getMaxBatchSize()).thenReturn(50L);
        when(config.getMaxEvents()).thenReturn(1000);
        when(handler.encodeEvent(any(Event.class))).thenReturn(ResourceSpans.getDefaultInstance());
        when(handler.getSerializedSize(any())).thenReturn(60L);

        buffer = new OtlpSinkBuffer(config, metrics, handler, sender, OtlpSignalType.TRACE);
        buffer.start();

        buffer.add(createMockSpanRecord());

        await().atMost(2, SECONDS).untilAsserted(() ->
            verify(sender, atLeast(1)).send(any(), any(OtlpSignalHandler.class), any(OtlpSignalType.class))
        );

        buffer.stop();
    }

    @Test
    void testStop_flushesRemainingBatch() throws Exception {
        when(config.getMaxEvents()).thenReturn(1000);
        when(config.getFlushTimeoutMillis()).thenReturn(10_000L);
        when(handler.encodeEvent(any(Event.class))).thenReturn(ResourceSpans.getDefaultInstance());
        when(handler.getSerializedSize(any())).thenReturn(10L);

        buffer = new OtlpSinkBuffer(config, metrics, handler, sender, OtlpSignalType.TRACE);
        buffer.start();

        buffer.add(createMockSpanRecord());

        // Give time for the event to be polled and encoded
        Thread.sleep(200);

        buffer.stop();

        // Final flush should have sent the remaining event
        await().atMost(2, SECONDS).untilAsserted(() ->
            verify(sender, atLeast(1)).send(any(), any(OtlpSignalHandler.class), any(OtlpSignalType.class))
        );
    }

    @Test
    void testIsRunning_returnsTrueAfterStart() {
        buffer.start();
        assertTrue(buffer.isRunning());
        buffer.stop();
    }

    @Test
    void testIsRunning_returnsFalseAfterStop() {
        buffer.start();
        buffer.stop();
        assertFalse(buffer.isRunning());
    }

    @Test
    void testRestartWorker_restartsWhenRunning() throws Exception {
        when(handler.encodeEvent(any(Event.class))).thenReturn(ResourceSpans.getDefaultInstance());
        when(handler.getSerializedSize(any())).thenReturn(10L);

        buffer.start();
        buffer.restartWorker();

        // Should still be able to process events after restart
        buffer.add(createMockSpanRecord());

        await().atMost(2, SECONDS).untilAsserted(() ->
            verify(sender, atLeast(1)).send(any(), any(OtlpSignalHandler.class), any(OtlpSignalType.class))
        );

        buffer.stop();
    }

    @Test
    void testRestartWorker_doesNotRestartWhenStopped() {
        buffer.start();
        buffer.stop();

        // Should not throw or restart since running is false
        buffer.restartWorker();

        assertFalse(buffer.isRunning());
    }

    @Test
    void testWorkerThread_interruptedWhileRunning_continuesProcessing() throws Exception {
        when(handler.encodeEvent(any(Event.class))).thenReturn(ResourceSpans.getDefaultInstance());
        when(handler.getSerializedSize(any())).thenReturn(10L);

        buffer.start();

        // Find and interrupt the worker thread
        Thread.sleep(100);
        for (final Thread t : Thread.getAllStackTraces().keySet()) {
            if (t.getName().contains("otlp-sink-buffer-thread")) {
                t.interrupt();
                break;
            }
        }

        // Give time for the interrupt to be handled
        Thread.sleep(200);

        // Buffer should still be running and able to process events
        assertTrue(buffer.isRunning());
        verify(metrics, atLeast(1)).incrementErrorsCount();

        buffer.stop();
    }

    @Test
    void testWorkerThread_crashesAndRestarts() throws Exception {
        // Make sender.send() throw an Error to crash the worker thread
        when(handler.encodeEvent(any(Event.class))).thenReturn(ResourceSpans.getDefaultInstance());
        when(handler.getSerializedSize(any())).thenReturn(10L);
        when(config.getMaxEvents()).thenReturn(1);

        buffer = new OtlpSinkBuffer(config, metrics, handler, sender, OtlpSignalType.TRACE);

        org.mockito.Mockito.doThrow(new OutOfMemoryError("test crash"))
                .when(sender).send(any(), any(OtlpSignalHandler.class), any(OtlpSignalType.class));

        buffer.start();
        buffer.add(createMockSpanRecord());

        // Wait for the crash and restart
        await().atMost(3, SECONDS).untilAsserted(() ->
            verify(metrics, atLeast(1)).incrementErrorsCount()
        );

        buffer.stop();
    }

    @Test
    void testAdd_interruptedWhileEnqueuing() throws Exception {
        // Configure a buffer with a tiny queue that will block on put()
        when(config.getMaxEvents()).thenReturn(0);
        buffer = new OtlpSinkBuffer(config, metrics, handler, sender, OtlpSignalType.TRACE);

        // Set the interrupt flag before calling add() so queue.put() throws InterruptedException
        Thread.currentThread().interrupt();

        buffer.add(createMockSpanRecord());

        verify(metrics).incrementFailedRecordsCount(1);
        verify(metrics).incrementErrorsCount();

        // Clear interrupt flag
        Thread.interrupted();
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
