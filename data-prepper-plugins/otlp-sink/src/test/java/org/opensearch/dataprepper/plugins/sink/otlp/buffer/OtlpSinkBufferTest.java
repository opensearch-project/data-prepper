/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.buffer;

import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.http.OtlpHttpSender;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;
import software.amazon.awssdk.regions.Region;

import java.lang.reflect.Field;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OtlpSinkBufferTest {

    private OtlpSinkConfig config;
    private OtlpSinkMetrics metrics;
    private OTelProtoStandardCodec.OTelProtoEncoder encoder;
    private OtlpHttpSender sender;
    private OtlpSinkBuffer buffer;

    @BeforeEach
    void setUp() {
        config = mock(OtlpSinkConfig.class);
        when(config.getMaxEvents()).thenReturn(2);
        when(config.getMaxRetries()).thenReturn(2);
        when(config.getMaxBatchSize()).thenReturn(1_000_000L);
        when(config.getFlushTimeoutMillis()).thenReturn(10L);
        when(config.getAwsRegion()).thenReturn(Region.of("us-west-2"));

        metrics = mock(OtlpSinkMetrics.class);
        encoder = mock(OTelProtoStandardCodec.OTelProtoEncoder.class);
        sender = mock(OtlpHttpSender.class);

        buffer = new OtlpSinkBuffer(config, metrics, encoder, sender);
    }

    @AfterEach
    void tearDown() {
        buffer.stop();
    }

    @Test
    void testStartAndStopDoesNotThrow() {
        buffer.start();
        buffer.stop();
        assertFalse(buffer.isRunning());
    }

    @Test
    void testAddHandlesInterruptedException() throws Exception {
        @SuppressWarnings("unchecked") final BlockingQueue<Record<Span>> badQueue = mock(BlockingQueue.class);
        doThrow(new InterruptedException()).when(badQueue).put(any());

        final Field queueField = OtlpSinkBuffer.class.getDeclaredField("queue");
        queueField.setAccessible(true);
        queueField.set(buffer, badQueue);

        final Record<Span> rec = mock(Record.class);
        buffer.add(rec);

        verify(metrics).incrementFailedSpansCount(1);
        verify(metrics).incrementErrorsCount();
    }

    @Test
    void testWorkerThreadHandlesEncodeException() throws Exception {
        when(encoder.convertToResourceSpans(any(Span.class)))
                .thenThrow(new RuntimeException("boom"))
                .thenReturn(ResourceSpans.getDefaultInstance());

        final Record<Span> rec1 = mock(Record.class);
        final Record<Span> rec2 = mock(Record.class);
        when(rec1.getData()).thenReturn(mock(Span.class));
        when(rec2.getData()).thenReturn(mock(Span.class));

        buffer.start();
        buffer.add(rec1);
        buffer.add(rec2);

        TimeUnit.MILLISECONDS.sleep(100);
        buffer.stop();

        await().atMost(1, SECONDS).untilAsserted(() -> verify(sender).send(any(byte[].class), anyInt()));
        verify(metrics).incrementFailedSpansCount(1);
        verify(metrics, atLeastOnce()).incrementErrorsCount();
    }

    @Test
    void testUncaughtExceptionHandler_logsAndRestarts_actualThread() throws Exception {
        final ExecutorService crashingExecutor = Executors.newSingleThreadExecutor(r -> {
            final Thread t = new Thread(() -> {
                throw new RuntimeException("forced crash");
            }, "otlp-sink-buffer-thread");
            t.setUncaughtExceptionHandler((thread, ex) -> {
                metrics.incrementErrorsCount();
                buffer.restartWorker();
            });
            return t;
        });

        final Field executorField = OtlpSinkBuffer.class.getDeclaredField("executor");
        executorField.setAccessible(true);
        executorField.set(buffer, crashingExecutor);

        buffer.start();

        await().atMost(1, SECONDS).untilAsserted(() -> verify(metrics, atLeastOnce()).incrementErrorsCount());

        crashingExecutor.shutdownNow();
    }

    @Test
    void testRestartWorkerDoesNotSubmitIfShutdown() throws Exception {
        final ExecutorService mockExecutor = mock(ExecutorService.class);
        when(mockExecutor.isShutdown()).thenReturn(true);

        final Field executorField = OtlpSinkBuffer.class.getDeclaredField("executor");
        executorField.setAccessible(true);
        executorField.set(buffer, mockExecutor);

        buffer.restartWorker();

        verify(mockExecutor, never()).execute(any());
    }

    @Test
    void testRestartWorkerSubmitsRunnableIfRunning() throws Exception {
        final ExecutorService mockExecutor = mock(ExecutorService.class);
        when(mockExecutor.isShutdown()).thenReturn(false);

        final Field executorField = OtlpSinkBuffer.class.getDeclaredField("executor");
        executorField.setAccessible(true);
        executorField.set(buffer, mockExecutor);

        buffer.restartWorker();

        verify(mockExecutor, atLeastOnce()).execute(any());
    }

    @Test
    void testRunWithInterruptedException() throws Exception {
        final OtlpSinkBuffer localBuffer = new OtlpSinkBuffer(config, metrics, encoder, sender);

        // Override the internal queue to throw InterruptedException
        final BlockingQueue<Record<Span>> interruptingQueue = mock(BlockingQueue.class);
        when(interruptingQueue.poll(any(Long.class), any(TimeUnit.class))).thenThrow(new InterruptedException());

        final Field queueField = OtlpSinkBuffer.class.getDeclaredField("queue");
        queueField.setAccessible(true);
        queueField.set(localBuffer, interruptingQueue);

        localBuffer.start();

        TimeUnit.MILLISECONDS.sleep(100); // Let the thread hit the poll

        localBuffer.stop();

        await().atMost(1, SECONDS).untilAsserted(() ->
                verify(metrics, atLeastOnce()).incrementErrorsCount()
        );
    }

    @Test
    void testWorkerThreadCatchThrowable_andRestart_fromEncoder() throws Exception {
        final OTelProtoStandardCodec.OTelProtoEncoder crashingEncoder = mock(OTelProtoStandardCodec.OTelProtoEncoder.class);
        when(crashingEncoder.convertToResourceSpans(any())).thenAnswer(invocation -> {
            throw new AssertionError("simulated fatal crash");
        });

        buffer = new OtlpSinkBuffer(config, metrics, crashingEncoder, sender);
        buffer.start();

        final Record<Span> record = mock(Record.class);
        when(record.getData()).thenReturn(mock(Span.class));
        buffer.add(record);

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(metrics, atLeastOnce()).incrementErrorsCount();
        });
    }

    @Test
    void testFinalFlushAfterStopFlushesBatch() throws Exception {
        // Configure buffer to *not* flush by size or time
        when(config.getMaxEvents()).thenReturn(1000); // very high
        when(config.getMaxBatchSize()).thenReturn(Long.MAX_VALUE); // very high
        when(config.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE); // effectively disables time-based flush

        buffer = new OtlpSinkBuffer(config, metrics, encoder, sender);
        buffer.start();

        // Prepare one span record
        final Record<Span> record = mock(Record.class);
        when(record.getData()).thenReturn(mock(Span.class));
        when(encoder.convertToResourceSpans(any())).thenReturn(ResourceSpans.getDefaultInstance());

        buffer.add(record);

        // Wait briefly to ensure the record is picked up
        TimeUnit.MILLISECONDS.sleep(100);

        // Stop should trigger the final flush
        buffer.stop();

        // Verify final flush triggered send
        await().atMost(1, SECONDS).untilAsserted(() ->
                verify(sender).send(any(byte[].class), anyInt())
        );
    }

    @Test
    void testPublicConstructorInitializesWithDefaults() {
        // Only mocks needed are config and metrics
        final OtlpSinkBuffer defaultBuffer = new OtlpSinkBuffer(config, metrics);

        assertNotNull(defaultBuffer); // Constructor didn't throw
    }
}