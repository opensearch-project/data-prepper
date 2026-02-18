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

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.sink.otlp.OtlpSignalHandler;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.http.OtlpHttpSender;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.utils.Pair;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A signal-agnostic back-pressure buffer for OTLP sink.
 * Uses a signal handler to encode and send events of a specific type.
 */
public class OtlpSinkBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(OtlpSinkBuffer.class);
    private static final int SAFETY_FACTOR = 10;
    private static final int MIN_QUEUE_CAPACITY = 2000;

    private final BlockingQueue<Record<Event>> queue;
    private final OtlpSignalHandler signalHandler;
    private final OtlpHttpSender sender;
    private final OtlpSinkMetrics sinkMetrics;

    private final int maxEvents;
    private final long maxBatchBytes;
    private final long flushTimeoutMillis;

    private final ExecutorService executor;

    @Getter
    private volatile boolean running = true;

    /**
     * Creates a new signal-specific OTLP sink buffer.
     *
     * @param config        the OTLP sink configuration
     * @param sinkMetrics   the metrics collector to use
     * @param signalHandler the signal handler for encoding and request building
     * @param sender        the HTTP sender
     */
    public OtlpSinkBuffer(@Nonnull final OtlpSinkConfig config,
                          @Nonnull final OtlpSinkMetrics sinkMetrics,
                          @Nonnull final OtlpSignalHandler signalHandler,
                          @Nonnull final OtlpHttpSender sender) {

        this.sinkMetrics = sinkMetrics;
        this.signalHandler = signalHandler;
        this.sender = sender;

        this.maxEvents = config.getMaxEvents();
        this.maxBatchBytes = config.getMaxBatchSize();
        this.flushTimeoutMillis = config.getFlushTimeoutMillis();

        this.queue = new LinkedBlockingQueue<>(getQueueCapacity());
        sinkMetrics.registerQueueGauges(queue);

        this.executor = Executors.newSingleThreadExecutor(r -> {
            final Thread t = new Thread(() -> {
                try {
                    r.run();
                } catch (final Throwable t1) {
                    LOG.error("Worker thread crashed unexpectedly", t1);
                    sinkMetrics.incrementErrorsCount();
                    restartWorker();
                }
            }, "otlp-sink-buffer-thread");
            t.setDaemon(false);
            return t;
        });
    }

    private int getQueueCapacity() {
        return Math.max(maxEvents * SAFETY_FACTOR, MIN_QUEUE_CAPACITY);
    }

    public void start() {
        running = true;
        executor.execute(this::run);
    }

    public void stop() {
        running = false;
        executor.shutdownNow();
    }

    @VisibleForTesting
    void restartWorker() {
        if (running && !executor.isShutdown()) {
            LOG.info("Restarting OTLP sink buffer worker thread");
            executor.execute(this::run);
        }
    }

    /**
     * Enqueues an event record for later batching and sending.
     * <p>
     * This will block if the internal queue is full, guaranteeing
     * lossless delivery during normal operations.
     * On interruption, the event is still rejected and
     * error metrics are incremented.
     *
     * @param record the event record to enqueue
     */
    public void add(final Record<Event> record) {
        try {
            queue.put(record);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while enqueuing event", e);
            sinkMetrics.incrementFailedRecordsCount(1);
            sinkMetrics.incrementErrorsCount();
        }
    }

    /**
     * Worker loop that batches events by count, size, or time and then flushes them.
     * <p>
     * Continues running as long as {@link #running} is true or the queue is not empty.
     * Handles encoding failures, timeout-based flush, and final flush on shutdown.
     */
    private void run() {
        final List<Pair<Object, EventHandle>> batch = new ArrayList<>();
        long batchSize = 0;
        long lastFlush = System.currentTimeMillis();

        while (true) {
            try {
                final long now = System.currentTimeMillis();
                final Record<Event> record = queue.poll(100, TimeUnit.MILLISECONDS);

                if (record != null) {
                    final Event event = record.getData();

                    try {
                        final Object encodedData = signalHandler.encodeEvent(event);
                        if (encodedData != null) {
                            final EventHandle eventHandle = event.getEventHandle();
                            batch.add(Pair.of(encodedData, eventHandle));
                            batchSize += signalHandler.getSerializedSize(encodedData);
                        }
                    } catch (final Exception e) {
                        LOG.error("Failed to encode event, skipping", e);
                        sinkMetrics.incrementFailedRecordsCount(1);
                        sinkMetrics.incrementErrorsCount();
                    }
                }

                final boolean flushBySize = (maxEvents > 0 && batch.size() >= maxEvents) || batchSize >= maxBatchBytes;
                final boolean flushByTime = !batch.isEmpty() && (now - lastFlush >= flushTimeoutMillis);

                if (flushBySize || flushByTime) {
                    sender.send(batch, signalHandler);
                    batch.clear();
                    batchSize = 0;
                    lastFlush = now;
                }

                if (!running && queue.isEmpty()) {
                    break;
                }

            } catch (final InterruptedException e) {
                if (running) {
                    LOG.debug("Worker interrupted while polling, continuing...");
                    sinkMetrics.incrementErrorsCount();
                }

                // Continue to loop if still running
            }
        }

        // Final flush
        if (!batch.isEmpty()) {
            sender.send(batch, signalHandler);
            batch.clear();
        }
    }
}
