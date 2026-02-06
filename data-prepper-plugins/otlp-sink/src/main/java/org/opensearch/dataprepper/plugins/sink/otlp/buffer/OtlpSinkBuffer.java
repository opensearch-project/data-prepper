/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.otlp.buffer;

import com.google.common.annotations.VisibleForTesting;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.trace.v1.ResourceSpans;
import lombok.Getter;
import org.opensearch.dataprepper.aws.api.AwsCredentialsSupplier;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventHandle;
import org.opensearch.dataprepper.model.log.Log;
import org.opensearch.dataprepper.model.metric.Metric;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.trace.Span;
import org.opensearch.dataprepper.plugins.otel.codec.OTelProtoStandardCodec;
import org.opensearch.dataprepper.plugins.sink.otlp.OtlpSignalType;
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
 * A back-pressure buffer for OTLP sink that supports traces, metrics, and logs.
 */
public class OtlpSinkBuffer {
    private static final Logger LOG = LoggerFactory.getLogger(OtlpSinkBuffer.class);
    private static final int SAFETY_FACTOR = 10;
    private static final int MIN_QUEUE_CAPACITY = 2000;

    private final BlockingQueue<Record<Event>> queue;
    private final OTelProtoStandardCodec.OTelProtoEncoder encoder;
    private final OtlpHttpSender sender;
    private final OtlpSinkMetrics sinkMetrics;

    private final int maxEvents;
    private final long maxBatchBytes;
    private final long flushTimeoutMillis;

    private final ExecutorService executor;

    @Getter
    private volatile boolean running = true;

    /**
     * Creates a new OTLP sink buffer.
     *
     * @param awsCredentialsSupplier the AWS credentials supplier
     * @param config      the OTLP sink configuration
     * @param sinkMetrics the metrics collector to use
     */
    public OtlpSinkBuffer(@Nonnull final AwsCredentialsSupplier awsCredentialsSupplier, @Nonnull final OtlpSinkConfig config, @Nonnull final OtlpSinkMetrics sinkMetrics) {
        this(config, sinkMetrics, new OTelProtoStandardCodec.OTelProtoEncoder(), new OtlpHttpSender(awsCredentialsSupplier, config, sinkMetrics));
    }

    /**
     * Visible for testing only: constructs an OTLP sink buffer with injected encoder and sender.
     */
    @VisibleForTesting
    OtlpSinkBuffer(@Nonnull final OtlpSinkConfig config,
                   @Nonnull final OtlpSinkMetrics sinkMetrics,
                   @Nonnull final OTelProtoStandardCodec.OTelProtoEncoder encoder,
                   @Nonnull final OtlpHttpSender sender) {

        this.sinkMetrics = sinkMetrics;
        this.encoder = encoder;
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
        OtlpSignalType currentSignalType = null;

        while (true) {
            try {
                final long now = System.currentTimeMillis();
                final Record<Event> record = queue.poll(100, TimeUnit.MILLISECONDS);

                if (record != null) {
                    final Event event = record.getData();
                    final OtlpSignalType signalType = OtlpSignalType.fromEvent(event);

                    // If signal type changes, flush current batch first
                    if (currentSignalType != null && currentSignalType != signalType && !batch.isEmpty()) {
                        sender.send(batch, currentSignalType);
                        batch.clear();
                        batchSize = 0;
                        lastFlush = now;
                    }

                    currentSignalType = signalType;

                    try {
                        final Object resourceData = encodeEvent(event, signalType);
                        if (resourceData != null) {
                            final EventHandle eventHandle = event.getEventHandle();
                            batch.add(Pair.of(resourceData, eventHandle));
                            batchSize += getSerializedSize(resourceData);
                        }
                    } catch (final Exception e) {
                        LOG.error("Failed to encode event of type {}, skipping", signalType, e);
                        sinkMetrics.incrementFailedRecordsCount(1);
                        sinkMetrics.incrementErrorsCount();
                    }
                }

                final boolean flushBySize = (maxEvents > 0 && batch.size() >= maxEvents) || batchSize >= maxBatchBytes;
                final boolean flushByTime = !batch.isEmpty() && (now - lastFlush >= flushTimeoutMillis);

                if (flushBySize || flushByTime) {
                    if (currentSignalType != null) {
                        sender.send(batch, currentSignalType);
                    }
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
        if (!batch.isEmpty() && currentSignalType != null) {
            sender.send(batch, currentSignalType);
            batch.clear();
        }
    }

    /**
     * Encodes an event based on its signal type.
     *
     * @param event the event to encode
     * @param signalType the signal type
     * @return the encoded resource data (ResourceSpans, ResourceMetrics, or ResourceLogs)
     */
    private Object encodeEvent(final Event event, final OtlpSignalType signalType) throws Exception {
        switch (signalType) {
            case TRACE:
                return encoder.convertToResourceSpans((Span) event);
            case METRIC:
                return encoder.convertToResourceMetrics((Metric) event);
            case LOG:
                return encoder.convertToResourceLogs((Log) event);
            default:
                LOG.warn("Unknown signal type: {}", signalType);
                return null;
        }
    }

    /**
     * Gets the serialized size of the resource data.
     *
     * @param resourceData the resource data
     * @return the serialized size in bytes
     */
    private long getSerializedSize(final Object resourceData) {
        if (resourceData instanceof ResourceSpans) {
            return ((ResourceSpans) resourceData).getSerializedSize();
        } else if (resourceData instanceof ResourceMetrics) {
            return ((ResourceMetrics) resourceData).getSerializedSize();
        } else if (resourceData instanceof ResourceLogs) {
            return ((ResourceLogs) resourceData).getSerializedSize();
        }
        return 0;
    }
}
