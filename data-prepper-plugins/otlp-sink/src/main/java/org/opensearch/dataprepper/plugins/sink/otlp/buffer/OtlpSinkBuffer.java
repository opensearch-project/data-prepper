/*
 *  Copyright OpenSearch Contributors
 *  SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.buffer;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.sink.otlp.configuration.OtlpSinkConfig;
import org.opensearch.dataprepper.plugins.sink.otlp.http.OtlpHttpSender;
import org.opensearch.dataprepper.plugins.sink.otlp.metrics.OtlpSinkMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A lossless, back-pressure aware buffer for OTLP sink.
 * <p>
 * Spans submitted via {@link #add(Record)} are enqueued, batched by count, size or time,
 * encoded to ResourceSpans, and flushed asynchronously over HTTP.
 */
public class OtlpSinkBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(OtlpSinkBuffer.class);
    private static final int SAFETY_FACTOR = 10;
    private static final int MIN_QUEUE_CAPACITY = 2000;

    private final BlockingQueue<Record<Span>> queue;
    private final OTelProtoStandardCodec.OTelProtoEncoder encoder;
    private final OtlpHttpSender sender;
    private final OtlpSinkMetrics sinkMetrics;

    private final int maxEvents;
    private final long maxBatchBytes;
    private final long flushTimeoutMillis;

    private final Thread workerThread;
    private volatile boolean running = true;

    /**
     * Creates a new OTLP sink buffer using default encoder and HTTP sender.
     *
     * @param config      the OTLP sink configuration
     * @param sinkMetrics the metrics collector to use
     */
    public OtlpSinkBuffer(@Nonnull final OtlpSinkConfig config, @Nonnull final OtlpSinkMetrics sinkMetrics) {
        this(config, sinkMetrics, null, null);
    }

    /**
     * Visible for testing only: constructs an OTLP sink buffer with injected encoder and sender.
     *
     * @param config      the OTLP sink configuration
     * @param sinkMetrics the metrics collector
     * @param encoder     custom OTLP encoder (or null to use default)
     * @param sender      custom HTTP sender (or null to use default)
     */
    @VisibleForTesting
    OtlpSinkBuffer(@Nonnull final OtlpSinkConfig config,
                   @Nonnull final OtlpSinkMetrics sinkMetrics,
                   final OTelProtoStandardCodec.OTelProtoEncoder encoder,
                   final OtlpHttpSender sender) {

        this.sinkMetrics = sinkMetrics;
        this.encoder = encoder != null ? encoder : new OTelProtoStandardCodec.OTelProtoEncoder();
        this.sender = sender != null ? sender : new OtlpHttpSender(config, sinkMetrics);

        this.maxEvents = config.getMaxEvents();
        this.maxBatchBytes = config.getMaxBatchSize();
        this.flushTimeoutMillis = config.getFlushTimeoutMillis();

        this.queue = new LinkedBlockingQueue<>(getQueueCapacity());
        sinkMetrics.registerQueueGauges(queue);

        this.workerThread = new Thread(this::run, "otlp-sink-buffer-thread");
    }

    private int getQueueCapacity() {
        return Math.max(maxEvents * SAFETY_FACTOR, MIN_QUEUE_CAPACITY);
    }

    public boolean isRunning() {
        return running && workerThread.isAlive();
    }

    public void start() {
        running = true;
        workerThread.start();
    }

    public void stop() {
        running = false;
        workerThread.interrupt();
    }

    /**
     * Enqueues a span record for later batching and sending.
     * <p>
     * This will block if the internal queue is full, guaranteeing
     * lossless delivery. On interruption, the span is rejected and
     * error metrics are incremented.
     *
     * @param record the span record to enqueue
     */
    public void add(final Record<Span> record) {
        try {
            queue.put(record); // block until space available; guaranteeing lossless delivery
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.error("Interrupted while enqueuing span", e);
            sinkMetrics.incrementRejectedSpansCount(1);
            sinkMetrics.incrementErrorsCount();
        }
    }

    /**
     * Worker loop that batches spans by count, size, or time and then flushes them.
     * <p>
     * Continues running as long as {@link #running} is true or the queue is not empty.
     * Handles encoding failures, timeout-based flush, and final flush on shutdown.
     */
    private void run() {
        final List<ResourceSpans> batch = new ArrayList<>();
        long batchSize = 0;
        long lastFlush = System.currentTimeMillis();

        while (true) {
            try {
                final long now = System.currentTimeMillis();
                final Record<Span> record = queue.poll(100, TimeUnit.MILLISECONDS);

                if (record != null) {
                    try {
                        final ResourceSpans resourceSpans = encoder.convertToResourceSpans(record.getData());
                        batch.add(resourceSpans);
                        batchSize += resourceSpans.getSerializedSize();
                    } catch (final Exception e) {
                        LOG.error("Failed to encode span, skipping", e);
                        sinkMetrics.incrementRejectedSpansCount(1);
                        sinkMetrics.incrementErrorsCount();
                    }
                }

                final boolean flushBySize = batch.size() >= maxEvents || batchSize >= maxBatchBytes;
                final boolean flushByTime = !batch.isEmpty() && (now - lastFlush >= flushTimeoutMillis);

                if (flushBySize || flushByTime) {
                    send(batch);
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
                // Clear interrupt flag to allow queue.poll() again
                // Don't break; allow draining
            }
        }

        // Final flush
        if (!batch.isEmpty()) {
            send(batch);
        }
    }

    /**
     * Builds an ExportTraceServiceRequest from the given batch, sends it over HTTP,
     * and updates metrics on success or failure.
     * <p>
     * The batch is cleared in all cases to prepare for the next batch.
     *
     * @param batch the list of ResourceSpans to send
     */
    private void send(final List<ResourceSpans> batch) {
        final ExportTraceServiceRequest request = ExportTraceServiceRequest.newBuilder()
                .addAllResourceSpans(batch)
                .build();
        final byte[] payload = request.toByteArray();

        try {
            sender.send(payload);
            sinkMetrics.incrementRecordsOut(batch.size());
        } catch (final IOException e) {
            LOG.error("Failed to send payload.", e);
            sinkMetrics.incrementRejectedSpansCount(batch.size());
            sinkMetrics.incrementErrorsCount();
        } finally {
            batch.clear();
        }
    }
}
