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

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
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
        when(config.getMaxBatchSize()).thenReturn(1_000_000L);
        when(config.getFlushTimeoutMillis()).thenReturn(10L);
        when(config.getAwsRegion()).thenReturn(Region.of("us-west-2"));

        metrics = mock(OtlpSinkMetrics.class);
        encoder = mock(OTelProtoStandardCodec.OTelProtoEncoder.class);
        sender = mock(OtlpHttpSender.class);

        buffer = new OtlpSinkBuffer(config, metrics, encoder, sender);
        buffer.start();
    }

    @AfterEach
    void tearDown() {
        buffer.stop();
    }
    
    @Test
    void testIsRunningBeforeStartAndAfterStop() throws Exception {
        // create a fresh buffer (not started)
        final OtlpSinkBuffer localBuffer = new OtlpSinkBuffer(config, metrics, encoder, sender);
        assertFalse(localBuffer.isRunning(), "not running until start() is called");

        localBuffer.start();
        // give the thread a moment to spin up
        TimeUnit.MILLISECONDS.sleep(50);
        assertTrue(localBuffer.isRunning(), "should be running immediately after start()");

        localBuffer.stop();
        // give the thread time to terminate
        TimeUnit.MILLISECONDS.sleep(50);
        assertFalse(localBuffer.isRunning(), "should stop after stop()");
    }

    @Test
    void testAddHandlesInterruptedException() throws Exception {
        // create a mock queue that throws when put(...) is called
        @SuppressWarnings("unchecked") final BlockingQueue<Record<Span>> badQueue = mock(BlockingQueue.class);
        doThrow(new InterruptedException()).when(badQueue).put(any());

        // inject it via reflection
        final Field queueField = OtlpSinkBuffer.class.getDeclaredField("queue");
        queueField.setAccessible(true);
        queueField.set(buffer, badQueue);

        final Record<Span> rec = mock(Record.class);
        buffer.add(rec);

        // should have hit the InterruptedException branch
        verify(metrics).incrementRejectedSpansCount(1);
        verify(metrics).incrementErrorsCount();
    }

    @Test
    void testFinalFlushOnShutdownWhenNoSizeOrTimeFlush() throws Exception {
        // new config: very large batch/time so we never flush by size/time
        final OtlpSinkConfig largeConfig = mock(OtlpSinkConfig.class);
        when(largeConfig.getMaxEvents()).thenReturn(100);
        when(largeConfig.getMaxBatchSize()).thenReturn(Long.MAX_VALUE);
        when(largeConfig.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE);

        final OtlpSinkBuffer finalBuffer = new OtlpSinkBuffer(largeConfig, metrics, encoder, sender);
        finalBuffer.start();

        // enqueue exactly one record
        final Record<Span> rec = mock(Record.class);
        when(rec.getData()).thenReturn(mock(Span.class));
        final ResourceSpans rs = ResourceSpans.getDefaultInstance();
        when(encoder.convertToResourceSpans(any(Span.class))).thenReturn(rs);
        finalBuffer.add(rec);

        // now shutdown
        finalBuffer.stop();
        TimeUnit.MILLISECONDS.sleep(50);

        // final‐flush should happen exactly once
        await().atMost(1, SECONDS).untilAsserted(() ->
                verify(sender).send(any(byte[].class))
        );
        verify(metrics).incrementRecordsOut(1);
    }

    @Test
    void testSendIoExceptionIncrementsRejectedAndError() throws Exception {
        // Prepare a tiny batch so that send(...) will be invoked
        final ResourceSpans rs = ResourceSpans.getDefaultInstance();
        when(encoder.convertToResourceSpans(any(Span.class))).thenReturn(rs);
        doThrow(new IOException("uh-oh")).when(sender).send(any(byte[].class));

        // Enqueue two spans to hit batch-size flush
        for (int i = 0; i < 2; i++) {
            final Record<Span> rec = mock(Record.class);
            when(rec.getData()).thenReturn(mock(Span.class));
            buffer.add(rec);
        }

        // Give worker thread time to flush by size
        TimeUnit.MILLISECONDS.sleep(50);
        buffer.stop();

        // verify send was attempted
        verify(sender).send(any(byte[].class));
        // one batch of 2 spans failed: rejected count should be 2
        verify(metrics).incrementRejectedSpansCount(2);
        // error count for the IO error (+ possibly one more when interrupted)
        verify(metrics, atLeastOnce()).incrementErrorsCount();
    }

    @Test
    void testWorkerThreadFlushesBySize() throws Exception {
        final ResourceSpans rs = ResourceSpans.getDefaultInstance();
        when(encoder.convertToResourceSpans(any(Span.class))).thenReturn(rs);

        // Enqueue exactly maxEvents (2) spans
        for (int i = 0; i < 2; i++) {
            final Record<Span> rec = mock(Record.class);
            when(rec.getData()).thenReturn(mock(Span.class));
            buffer.add(rec);
        }

        TimeUnit.MILLISECONDS.sleep(50);
        buffer.stop();

        // at least one send of our 2-item batch
        verify(sender, atLeastOnce()).send(any(byte[].class));
        // at least one successful record-out of 2
        verify(metrics, atLeastOnce()).incrementRecordsOut(2);
    }

    @Test
    void testQueueCapacityRespectsMinimum() throws Exception {
        when(config.getMaxEvents()).thenReturn(1);
        buffer = new OtlpSinkBuffer(config, metrics, encoder, sender);

        final Field queueField = OtlpSinkBuffer.class.getDeclaredField("queue");
        queueField.setAccessible(true);
        final BlockingQueue<?> queueInstance = (BlockingQueue<?>) queueField.get(buffer);
        assertEquals(2000, queueInstance.remainingCapacity());
    }

    @Test
    void testQueueCapacityBasedOnMaxEvents() throws Exception {
        when(config.getMaxEvents()).thenReturn(300);
        buffer = new OtlpSinkBuffer(config, metrics, encoder, sender);

        final Field queueField = OtlpSinkBuffer.class.getDeclaredField("queue");
        queueField.setAccessible(true);
        final BlockingQueue<?> queueInstance = (BlockingQueue<?>) queueField.get(buffer);
        assertEquals(300 * 10, queueInstance.remainingCapacity());
    }

    @Test
    void testWorkerThreadFlushesByBatchByteSize() throws Exception {
        // Arrange: make batchSize threshold zero so any non-null ResourceSpans triggers flush
        when(config.getMaxEvents()).thenReturn(10);
        when(config.getMaxBatchSize()).thenReturn(0L);
        when(config.getFlushTimeoutMillis()).thenReturn(Long.MAX_VALUE);

        // restart with new config
        buffer.stop();
        buffer = new OtlpSinkBuffer(config, metrics, encoder, sender);
        buffer.start();

        when(encoder.convertToResourceSpans(any(Span.class)))
                .thenReturn(ResourceSpans.getDefaultInstance());

        // Act: add one record
        final Record<Span> rec = mock(Record.class);
        when(rec.getData()).thenReturn(mock(Span.class));
        buffer.add(rec);

        // give the worker a moment to do both the immediate flush and then exit
        TimeUnit.MILLISECONDS.sleep(50);
        buffer.stop();

        // Assert: we expect at least one send (could be two)
        verify(sender, atLeastOnce()).send(any(byte[].class));
        verify(metrics, atLeastOnce()).incrementRecordsOut(1);
    }

    @Test
    void testWorkerThreadHandlesEncodeException() throws Exception {
        // Bad span first
        when(encoder.convertToResourceSpans(any(Span.class)))
                .thenThrow(new RuntimeException("boom"))
                .thenReturn(ResourceSpans.getDefaultInstance());

        // Enqueue two spans: one bad, one good
        for (int i = 0; i < 2; i++) {
            final Record<Span> rec = mock(Record.class);
            when(rec.getData()).thenReturn(mock(Span.class));
            buffer.add(rec);
        }

        TimeUnit.MILLISECONDS.sleep(50);
        buffer.stop();

        // should still send at least one batch for the good span
        await().atMost(1, SECONDS).untilAsserted(() ->
                verify(sender).send(any(byte[].class))
        );
        // one rejected from the encode exception
        verify(metrics).incrementRejectedSpansCount(1);
        // error count for the encode exception (+ maybe one on interrupt)
        verify(metrics, atLeastOnce()).incrementErrorsCount();
        // at least one successful record-out of 1
        verify(metrics, atLeastOnce()).incrementRecordsOut(1);
    }

    @Test
    void testConstructorDefaults() throws Exception {
        // use the two-arg constructor
        final OtlpSinkBuffer defaultBuf = new OtlpSinkBuffer(config, metrics);

        // reflectively inspect encoder and sender
        final Field encField = OtlpSinkBuffer.class.getDeclaredField("encoder");
        final Field sndField = OtlpSinkBuffer.class.getDeclaredField("sender");
        encField.setAccessible(true);
        sndField.setAccessible(true);

        final Object enc = encField.get(defaultBuf);
        final Object snd = sndField.get(defaultBuf);

        assertNotNull(enc, "default encoder should not be null");
        assertInstanceOf(OTelProtoStandardCodec.OTelProtoEncoder.class, enc);

        assertNotNull(snd, "default sender should not be null");
        assertInstanceOf(OtlpHttpSender.class, snd);
    }

    @Test
    void testWorkerThreadFlushesByTimeoutOnly() throws Exception {
        when(config.getMaxEvents()).thenReturn(100); // high enough not to flush by count
        when(config.getMaxBatchSize()).thenReturn(Long.MAX_VALUE); // don't flush by size
        when(config.getFlushTimeoutMillis()).thenReturn(50L); // very short flush window

        buffer.stop();
        buffer = new OtlpSinkBuffer(config, metrics, encoder, sender);
        buffer.start();

        final Record<Span> rec = mock(Record.class);
        when(rec.getData()).thenReturn(mock(Span.class));
        when(encoder.convertToResourceSpans(any(Span.class))).thenReturn(ResourceSpans.getDefaultInstance());

        buffer.add(rec);

        // wait long enough to trigger timeout-based flush
        TimeUnit.MILLISECONDS.sleep(100);
        buffer.stop();

        await().atMost(1, SECONDS).untilAsserted(() ->
                verify(sender).send(any(byte[].class))
        );
        verify(metrics, atLeastOnce()).incrementRecordsOut(1);
    }
}