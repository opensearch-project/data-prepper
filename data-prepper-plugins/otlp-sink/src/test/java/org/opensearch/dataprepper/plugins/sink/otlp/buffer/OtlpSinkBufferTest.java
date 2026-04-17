/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.buffer;

import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.sink.otlp.codec.OtlpLogEncoder;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class OtlpSinkBufferTest {

    private OtlpSinkConfig config;
    private OtlpSinkMetrics metrics;
    private OTelProtoStandardCodec.OTelProtoEncoder traceEncoder;
    private OtlpLogEncoder logEncoder;
    private OtlpHttpSender sender;
    private OtlpSinkBuffer buffer;
    private AwsCredentialsSupplier mockAwsCredSupplier;

    @BeforeEach
    void setUp() {
        config = mock(OtlpSinkConfig.class);
        when(config.getEndpoint()).thenReturn("https://localhost/v1/traces");
        when(config.getMaxEvents()).thenReturn(2);
        when(config.getMaxRetries()).thenReturn(2);
        when(config.getMaxBatchSize()).thenReturn(1_000_000L);
        when(config.getFlushTimeoutMillis()).thenReturn(10L);
        when(config.getAwsRegion()).thenReturn(Region.of("us-west-2"));
        when(config.isLogSignal()).thenReturn(false);

        metrics = mock(OtlpSinkMetrics.class);
        traceEncoder = mock(OTelProtoStandardCodec.OTelProtoEncoder.class);
        logEncoder = mock(OtlpLogEncoder.class);
        sender = mock(OtlpHttpSender.class);
        mockAwsCredSupplier = mock(AwsCredentialsSupplier.class);

        buffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
    }

    @AfterEach
    void tearDown() {
        if (buffer != null) {
            buffer.stop();
        }
    }

    @Test
    void testStartAndStopDoesNotThrow() {
        buffer.start();
        assertTrue(buffer.isRunning());
        buffer.stop();
        assertFalse(buffer.isRunning());
    }

    @Test
    void testPublicConstructorInitializesWithDefaults() {
        final OtlpSinkBuffer defaultBuffer = new OtlpSinkBuffer(mockAwsCredSupplier, config, metrics);
        assertNotNull(defaultBuffer);
        assertTrue(defaultBuffer.isRunning());
        defaultBuffer.stop();
    }

    @Test
    void testQueueCapacityCalculation_withHighMaxEvents() {
        when(config.getMaxEvents()).thenReturn(500);
        final OtlpSinkBuffer highCapacityBuffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
        assertNotNull(highCapacityBuffer);
        highCapacityBuffer.stop();
    }

    @Test
    void testQueueCapacityCalculation_withLowMaxEvents() {
        when(config.getMaxEvents()).thenReturn(1);
        final OtlpSinkBuffer lowCapacityBuffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
        assertNotNull(lowCapacityBuffer);
        lowCapacityBuffer.stop();
    }

    @Test
    void testQueueGaugesRegistration() {
        verify(metrics).registerQueueGauges(any(BlockingQueue.class));
    }

    @Test
    void testAddHandlesInterruptedException() throws Exception {
        @SuppressWarnings("unchecked") final BlockingQueue<Record<Event>> badQueue = mock(BlockingQueue.class);
        doThrow(new InterruptedException()).when(badQueue).put(any());

        final Field queueField = OtlpSinkBuffer.class.getDeclaredField("queue");
        queueField.setAccessible(true);
        queueField.set(buffer, badQueue);

        final Record<Event> rec = mock(Record.class);
        buffer.add(rec);

        verify(metrics).incrementFailedSpansCount(1);
        verify(metrics).incrementErrorsCount();
    }

    @Test
    void testWorkerThreadHandlesEncodeException() throws Exception {
        when(traceEncoder.convertToResourceSpans(any(Span.class)))
                .thenThrow(new RuntimeException("boom"))
                .thenReturn(ResourceSpans.getDefaultInstance());

        final Record<Event> rec1 = createMockSpanRecord();
        final Record<Event> rec2 = createMockSpanRecord();

        buffer.start();
        buffer.add(rec1);
        buffer.add(rec2);

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(metrics).incrementFailedSpansCount(1);
            verify(metrics, atLeastOnce()).incrementErrorsCount();
        });

        buffer.stop();

        await().atMost(1, SECONDS).untilAsserted(() -> verify(sender).send(anyList(), eq(false)));
    }

    @Test
    void testFlushByEventCount() throws Exception {
        when(config.getMaxEvents()).thenReturn(2);
        when(config.getMaxBatchSize()).thenReturn(Long.MAX_VALUE);
        when(config.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE);

        when(traceEncoder.convertToResourceSpans(any(Span.class))).thenReturn(ResourceSpans.getDefaultInstance());

        buffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
        buffer.start();

        buffer.add(createMockSpanRecord());
        buffer.add(createMockSpanRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> verify(sender).send(anyList(), eq(false)));
        buffer.stop();
    }

    @Test
    void testFlushByBatchSize() throws Exception {
        when(config.getMaxEvents()).thenReturn(Integer.MAX_VALUE);
        when(config.getMaxBatchSize()).thenReturn(100L);
        when(config.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE);

        final ResourceSpans largeResourceSpans = mock(ResourceSpans.class);
        when(largeResourceSpans.getSerializedSize()).thenReturn(150);
        when(traceEncoder.convertToResourceSpans(any(Span.class))).thenReturn(largeResourceSpans);

        buffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
        buffer.start();

        buffer.add(createMockSpanRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> verify(sender).send(anyList(), eq(false)));
        buffer.stop();
    }

    @Test
    void testFlushByTimeout() throws Exception {
        when(config.getMaxEvents()).thenReturn(Integer.MAX_VALUE);
        when(config.getMaxBatchSize()).thenReturn(Long.MAX_VALUE);
        when(config.getFlushTimeoutMillis()).thenReturn(50L);

        when(traceEncoder.convertToResourceSpans(any(Span.class))).thenReturn(ResourceSpans.getDefaultInstance());

        buffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
        buffer.start();

        buffer.add(createMockSpanRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> verify(sender).send(anyList(), eq(false)));
        buffer.stop();
    }

    @Test
    void testWorkerLoopWithEmptyQueue() throws Exception {
        when(config.getMaxEvents()).thenReturn(Integer.MAX_VALUE);
        when(config.getMaxBatchSize()).thenReturn(Long.MAX_VALUE);
        when(config.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE);

        buffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
        buffer.start();

        TimeUnit.MILLISECONDS.sleep(200);

        buffer.stop();
        verify(sender, never()).send(anyList(), anyBoolean());
    }

    @Test
    void testEventHandleIncludedInBatch() throws Exception {
        when(traceEncoder.convertToResourceSpans(any(Span.class))).thenReturn(ResourceSpans.getDefaultInstance());

        final EventHandle eventHandle = mock(EventHandle.class);
        final Span span = mock(Span.class);
        when(span.getEventHandle()).thenReturn(eventHandle);
        @SuppressWarnings("unchecked") final Record<Event> rec = mock(Record.class);
        when(rec.getData()).thenReturn(span);

        buffer.start();
        buffer.add(rec);

        TimeUnit.MILLISECONDS.sleep(100);
        buffer.stop();

        await().atMost(1, SECONDS).untilAsserted(() -> verify(sender).send(anyList(), eq(false)));
        verify(span).getEventHandle();
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
    void testRestartWorkerDoesNotSubmitIfNotRunning() throws Exception {
        final ExecutorService mockExecutor = mock(ExecutorService.class);
        when(mockExecutor.isShutdown()).thenReturn(false);

        final Field executorField = OtlpSinkBuffer.class.getDeclaredField("executor");
        executorField.setAccessible(true);
        executorField.set(buffer, mockExecutor);

        buffer.stop();

        buffer.restartWorker();

        verify(mockExecutor, never()).execute(any());
    }

    @Test
    void testRunWithInterruptedException() throws Exception {
        final OtlpSinkBuffer localBuffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);

        @SuppressWarnings("unchecked") final BlockingQueue<Record<Event>> interruptingQueue = mock(BlockingQueue.class);
        when(interruptingQueue.poll(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException());
        when(interruptingQueue.isEmpty()).thenReturn(true);

        final Field queueField = OtlpSinkBuffer.class.getDeclaredField("queue");
        queueField.setAccessible(true);
        queueField.set(localBuffer, interruptingQueue);

        localBuffer.start();

        TimeUnit.MILLISECONDS.sleep(100);

        localBuffer.stop();

        await().atMost(1, SECONDS).untilAsserted(() ->
                verify(metrics, atLeastOnce()).incrementErrorsCount()
        );
    }

    @Test
    void testRunWithInterruptedException_whileRunning() throws Exception {
        final OtlpSinkBuffer localBuffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);

        @SuppressWarnings("unchecked") final BlockingQueue<Record<Event>> interruptingQueue = mock(BlockingQueue.class);
        when(interruptingQueue.poll(anyLong(), any(TimeUnit.class)))
                .thenThrow(new InterruptedException())
                .thenReturn(null);
        when(interruptingQueue.isEmpty()).thenReturn(false, true);

        final Field queueField = OtlpSinkBuffer.class.getDeclaredField("queue");
        queueField.setAccessible(true);
        queueField.set(localBuffer, interruptingQueue);

        localBuffer.start();

        TimeUnit.MILLISECONDS.sleep(200);

        localBuffer.stop();

        await().atMost(2, SECONDS).untilAsserted(() ->
                verify(metrics, atLeastOnce()).incrementErrorsCount()
        );
    }

    @Test
    void testWorkerThreadCatchThrowable_andRestart_fromEncoder() throws Exception {
        final OTelProtoStandardCodec.OTelProtoEncoder crashingEncoder = mock(OTelProtoStandardCodec.OTelProtoEncoder.class);
        when(crashingEncoder.convertToResourceSpans(any())).thenAnswer(invocation -> {
            throw new AssertionError("simulated fatal crash");
        });

        buffer = new OtlpSinkBuffer(config, metrics, crashingEncoder, logEncoder, sender);
        buffer.start();

        buffer.add(createMockSpanRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(metrics, atLeastOnce()).incrementErrorsCount();
        });
    }

    @Test
    void testFinalFlushAfterStopFlushesBatch() throws Exception {
        when(config.getMaxEvents()).thenReturn(1000);
        when(config.getMaxBatchSize()).thenReturn(Long.MAX_VALUE);
        when(config.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE);

        when(traceEncoder.convertToResourceSpans(any())).thenReturn(ResourceSpans.getDefaultInstance());

        buffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
        buffer.start();

        buffer.add(createMockSpanRecord());

        TimeUnit.MILLISECONDS.sleep(100);

        buffer.stop();

        await().atMost(1, SECONDS).untilAsserted(() ->
                verify(sender).send(anyList(), eq(false))
        );
    }

    @Test
    void testNoFinalFlushWhenBatchIsEmpty() throws Exception {
        when(config.getMaxEvents()).thenReturn(1000);
        when(config.getMaxBatchSize()).thenReturn(Long.MAX_VALUE);
        when(config.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE);

        buffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
        buffer.start();

        TimeUnit.MILLISECONDS.sleep(50);
        buffer.stop();

        verify(sender, never()).send(anyList(), anyBoolean());
    }

    @Test
    void testMaxEventsZeroDoesNotTriggerFlush() throws Exception {
        when(config.getMaxEvents()).thenReturn(0);
        when(config.getMaxBatchSize()).thenReturn(Long.MAX_VALUE);
        when(config.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE);

        when(traceEncoder.convertToResourceSpans(any())).thenReturn(ResourceSpans.getDefaultInstance());

        buffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
        buffer.start();

        buffer.add(createMockSpanRecord());

        TimeUnit.MILLISECONDS.sleep(100);
        buffer.stop();

        await().atMost(1, SECONDS).untilAsserted(() ->
                verify(sender, times(1)).send(anyList(), eq(false))
        );
    }

    @Test
    void testDaemonThreadConfiguration() {
        buffer.start();
        assertTrue(buffer.isRunning());
        buffer.stop();
    }

    @Test
    void testBatchSizeAccumulation() throws Exception {
        when(config.getMaxEvents()).thenReturn(Integer.MAX_VALUE);
        when(config.getMaxBatchSize()).thenReturn(50L);
        when(config.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE);

        final ResourceSpans resourceSpans1 = mock(ResourceSpans.class);
        final ResourceSpans resourceSpans2 = mock(ResourceSpans.class);
        when(resourceSpans1.getSerializedSize()).thenReturn(20);
        when(resourceSpans2.getSerializedSize()).thenReturn(35);

        when(traceEncoder.convertToResourceSpans(any(Span.class)))
                .thenReturn(resourceSpans1)
                .thenReturn(resourceSpans2);

        buffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
        buffer.start();

        buffer.add(createMockSpanRecord());
        buffer.add(createMockSpanRecord());

        await().atMost(1, SECONDS).untilAsserted(() -> verify(sender).send(anyList(), eq(false)));
        buffer.stop();
    }

    @Test
    void testMultipleFlushCycles() throws Exception {
        when(config.getMaxEvents()).thenReturn(1);
        when(config.getMaxBatchSize()).thenReturn(Long.MAX_VALUE);
        when(config.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE);

        when(traceEncoder.convertToResourceSpans(any())).thenReturn(ResourceSpans.getDefaultInstance());

        buffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
        buffer.start();

        buffer.add(createMockSpanRecord());
        buffer.add(createMockSpanRecord());
        buffer.add(createMockSpanRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> verify(sender, times(3)).send(anyList(), eq(false)));
        buffer.stop();
    }

    @Test
    void testEncodingFailureDoesNotStopProcessing() throws Exception {
        when(config.getMaxEvents()).thenReturn(2);
        when(config.getMaxBatchSize()).thenReturn(Long.MAX_VALUE);
        when(config.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE);

        when(traceEncoder.convertToResourceSpans(any(Span.class)))
                .thenReturn(ResourceSpans.getDefaultInstance())
                .thenThrow(new RuntimeException("encoding error"))
                .thenReturn(ResourceSpans.getDefaultInstance());

        buffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
        buffer.start();

        buffer.add(createMockSpanRecord());
        buffer.add(createMockSpanRecord());
        buffer.add(createMockSpanRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(sender).send(anyList(), eq(false));
            verify(metrics).incrementFailedSpansCount(1);
            verify(metrics, atLeastOnce()).incrementErrorsCount();
        });

        buffer.stop();
    }

    // --- Log signal tests ---

    @Test
    void testLogSignal_usesLogEncoder() throws Exception {
        when(config.isLogSignal()).thenReturn(true);
        when(config.getMaxEvents()).thenReturn(1);
        when(config.getMaxBatchSize()).thenReturn(Long.MAX_VALUE);
        when(config.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE);

        when(logEncoder.encode(any(Event.class))).thenReturn(ResourceLogs.getDefaultInstance());

        buffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
        buffer.start();

        buffer.add(createMockEventRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> verify(sender).send(anyList(), eq(true)));
        verify(logEncoder).encode(any(Event.class));
        buffer.stop();
    }

    @Test
    void testLogSignal_encodingFailureIncrementsMetrics() throws Exception {
        when(config.isLogSignal()).thenReturn(true);
        when(config.getMaxEvents()).thenReturn(1);
        when(config.getMaxBatchSize()).thenReturn(Long.MAX_VALUE);
        when(config.getFlushTimeoutMillis()).thenReturn(50L);

        when(logEncoder.encode(any(Event.class))).thenThrow(new RuntimeException("log encode failed"));

        buffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
        buffer.start();

        buffer.add(createMockEventRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(metrics).incrementFailedSpansCount(1);
            verify(metrics, atLeastOnce()).incrementErrorsCount();
        });

        buffer.stop();
    }

    @Test
    void testTraceSignal_rejectsNonSpanEvent() throws Exception {
        when(config.isLogSignal()).thenReturn(false);
        when(config.getMaxEvents()).thenReturn(1);
        when(config.getMaxBatchSize()).thenReturn(Long.MAX_VALUE);
        when(config.getFlushTimeoutMillis()).thenReturn(50L);

        buffer = new OtlpSinkBuffer(config, metrics, traceEncoder, logEncoder, sender);
        buffer.start();

        // Add a plain Event (not a Span) in trace mode
        buffer.add(createMockEventRecord());

        await().atMost(2, SECONDS).untilAsserted(() -> {
            verify(metrics).incrementFailedSpansCount(1);
            verify(metrics, atLeastOnce()).incrementErrorsCount();
        });

        buffer.stop();
    }

    private Record<Event> createMockSpanRecord() {
        @SuppressWarnings("unchecked") final Record<Event> record = mock(Record.class);
        final Span span = mock(Span.class);
        final EventHandle eventHandle = mock(EventHandle.class);
        when(record.getData()).thenReturn(span);
        when(span.getEventHandle()).thenReturn(eventHandle);
        return record;
    }

    private Record<Event> createMockEventRecord() {
        @SuppressWarnings("unchecked") final Record<Event> record = mock(Record.class);
        final Event event = mock(Event.class);
        final EventHandle eventHandle = mock(EventHandle.class);
        when(record.getData()).thenReturn(event);
        when(event.getEventHandle()).thenReturn(eventHandle);
        return record;
    }
}
