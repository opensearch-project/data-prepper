/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.buffer;

import io.micrometer.core.instrument.Counter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.opensearch.dataprepper.metrics.MetricNames;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.model.ReadBatch;
import org.opensearch.dataprepper.plugins.buffer.model.SignaledBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A synchronized buffer that provides a blocking write model where the caller
 * thread waits until its records have been fully processed by worker threads
 * and acknowledged via checkpointing. This ensures a direct, synchronous flow
 * of data from source to sink while still leveraging parallel processing
 * through Data Prepper's process worker threads.
 */
@DataPrepperPlugin(name = "synchronized", pluginType = Buffer.class, pluginConfigurationType = SynchronizedBufferConfig.class)
public class SynchronizedBuffer<T extends Record<?>> implements Buffer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(SynchronizedBuffer.class);
    private static final String PLUGIN_COMPONENT_ID = "SynchronizedBuffer";

    private final ConcurrentLinkedQueue<SignaledBatch<T>> queue = new ConcurrentLinkedQueue<>();
    private final ThreadLocal<List<ReadBatch<T>>> threadLocalReadBatches = new ThreadLocal<>();
    private final PluginMetrics pluginMetrics;
    private final Counter writeRecordsCounter;
    private final Counter readRecordsCounter;
    private final int batchSize;

    /**
     * Creates a new SynchronizedBuffer with the specified configuration.
     *
     * @param synchronizedBufferConfig The buffer configuration
     * @param pipelineDescription The pipeline description
     */
    @DataPrepperPluginConstructor
    public SynchronizedBuffer(final SynchronizedBufferConfig synchronizedBufferConfig, final PipelineDescription pipelineDescription) {
        this.pluginMetrics = PluginMetrics.fromNames(PLUGIN_COMPONENT_ID, pipelineDescription.getPipelineName());
        this.writeRecordsCounter = pluginMetrics.counter(MetricNames.RECORDS_WRITTEN);
        this.readRecordsCounter = pluginMetrics.counter(MetricNames.RECORDS_READ);
        this.batchSize = synchronizedBufferConfig.getBatchSize();
        LOG.info("Initialized SynchronizedBuffer with batch size: {}", this.batchSize);
    }

    /**
     * Writes a single record to the buffer and waits for it to be processed.
     *
     * @param record The record to write
     * @param timeoutInMillis How long to wait before giving up
     * @throws TimeoutException If the write times out
     */
    @Override
    public void write(T record, int timeoutInMillis) throws TimeoutException {
        if (record == null) {
            throw new NullPointerException("The write record cannot be null");
        }

        SignaledBatch<T> batch = new SignaledBatch<>(List.of(record));
        queue.offer(batch);
        writeRecordsCounter.increment();

        try {
            batch.getSignal().get(timeoutInMillis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new TimeoutException("Writer timed out waiting for checkpoint");
        }
    }

    /**
     * Writes multiple records to the buffer and waits for them to be processed.
     *
     * @param records The records to write
     * @param timeoutInMillis How long to wait before giving up
     * @throws Exception If the write fails or times out
     */
    @Override
    public void writeAll(Collection<T> records, int timeoutInMillis) throws Exception {
        if (records == null) {
            throw new NullPointerException("The write records cannot be null");
        }

        if (records.isEmpty()) {
            LOG.debug("The records are empty");
            return;
        }

        SignaledBatch<T> batch = new SignaledBatch<>(new ArrayList<>(records));
        queue.offer(batch);
        writeRecordsCounter.increment(records.size());

        try {
            batch.getSignal().get(timeoutInMillis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new TimeoutException("Writer timed out waiting for batch checkpoint");
        }
    }

    /**
     * Reads records from the buffer.
     *
     * @param timeoutInMillis How long to wait before giving up
     * @return A collection of records and their checkpoint state
     */
    @Override
    public Map.Entry<Collection<T>, CheckpointState> read(int timeoutInMillis) {
        final List<T> output = new ArrayList<>();
        final List<ReadBatch<T>> readBatches = new ArrayList<>();

        while (!queue.isEmpty() && output.size() < batchSize) {
            SignaledBatch<T> batch = queue.peek();
            if (batch == null) break;

            int canTake = batchSize - output.size();
            List<T> slice = batch.readNext(canTake);
            if (slice.isEmpty()) {
                queue.poll();
                continue;
            }

            output.addAll(slice);
            readBatches.add(new ReadBatch<>(batch, slice.size()));

            if (batch.getRemaining() == 0) {
                queue.poll();
            }
        }

        if (!output.isEmpty()) {
            readRecordsCounter.increment(output.size());
        }

        threadLocalReadBatches.set(readBatches);
        return Map.entry(output, new CheckpointState(0));
    }

    /**
     * Checkpoints the processed records, signaling completion to writers.
     *
     * @param checkpointState The checkpoint state
     */
    @Override
    public void checkpoint(final CheckpointState checkpointState) {
        List<ReadBatch<T>> readBatches = threadLocalReadBatches.get();
        if (readBatches != null) {
            for (ReadBatch<T> rb : readBatches) {
                rb.getBatch().markProcessed(rb.getCount());
            }
            threadLocalReadBatches.remove();
        }
    }

    /**
     * Checks if the buffer is empty.
     *
     * @return true if the buffer is empty, false otherwise
     */
    @Override
    public boolean isEmpty() {
        return queue.isEmpty();
    }
}
