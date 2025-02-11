/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.buffer.blockingbuffer;

import com.google.common.base.Stopwatch;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.AbstractBuffer;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBufferConfig.DEFAULT_BATCH_SIZE;
import static org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBufferConfig.DEFAULT_BUFFER_CAPACITY;

/**
 * A bounded BlockingBuffer is an implementation of {@link Buffer} using {@link LinkedBlockingQueue}, it is bounded
 * to the provided capacity {@link #ATTRIBUTE_BUFFER_CAPACITY} or {@link #ATTRIBUTE_BUFFER_CAPACITY} (if attribute is
 * not provided); {@link #write(Record, int)} inserts specified non-null record into this buffer, waiting up to the
 * specified timeout in milliseconds if necessary for space to become available; and throws an exception if the
 * record is null. {@link #read(int)} retrieves and removes the batch of records from the head of the queue. The
 * batch size is defined/determined by the configuration attribute {@link #ATTRIBUTE_BATCH_SIZE} or the timeout parameter
 */
@DataPrepperPlugin(name = "bounded_blocking", pluginType = Buffer.class, pluginConfigurationType = BlockingBufferConfig.class)
public class BlockingBuffer<T extends Record<?>> extends AbstractBuffer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(BlockingBuffer.class);
    private static final String PLUGIN_NAME = "bounded_blocking";
    private static final String ATTRIBUTE_BUFFER_CAPACITY = "buffer_size";
    private static final String ATTRIBUTE_BATCH_SIZE = "batch_size";
    private static final String BLOCKING_BUFFER = "BlockingBuffer";
    private static final String BUFFER_USAGE_METRIC = "bufferUsage";
    public static final String CAPACITY_USED_METRIC = "capacityUsed";
    private final int bufferCapacity;
    private final int batchSize;
    private final BlockingQueue<T> blockingQueue;
    private final String pipelineName;

    private final Semaphore capacitySemaphore;

    /**
     * Creates a BlockingBuffer with the given (fixed) capacity.
     *
     * @param bufferCapacity the capacity of the buffer
     * @param batchSize      the batch size for {@link #read(int)}
     * @param pipelineName   the name of the associated Pipeline
     */
    public BlockingBuffer(final int bufferCapacity, final int batchSize, final String pipelineName) {
        super(BLOCKING_BUFFER, pipelineName);
        this.bufferCapacity = bufferCapacity;
        this.batchSize = batchSize;
        this.blockingQueue = new LinkedBlockingQueue<>(bufferCapacity);
        this.capacitySemaphore = new Semaphore(bufferCapacity);
        this.pipelineName = pipelineName;

        PluginMetrics pluginMetrics = PluginMetrics.fromNames(BLOCKING_BUFFER, pipelineName);

        pluginMetrics.gauge(CAPACITY_USED_METRIC, capacitySemaphore, capacity -> bufferCapacity - capacity.availablePermits());
        pluginMetrics.gauge(BUFFER_USAGE_METRIC, capacitySemaphore, capacity -> ((double) bufferCapacity - capacity.availablePermits()) / bufferCapacity * 100);
    }

    /**
     * Mandatory constructor for Data Prepper Component - This constructor is used by Data Prepper runtime engine to construct an
     * instance of {@link BlockingBuffer} using an instance of {@link BlockingBufferConfig}. Buffer settings like `buffer-size`, `batch-size`,
     * `batch-timeout` are optional and can be passed via {@link BlockingBufferConfig}, if not present default values will
     * be used to create the buffer.
     *
     * @param blockingBufferConfig instance takes values from yaml
     * @param pipelineDescription  instance with metadata information aout pipeline man
     */
    @DataPrepperPluginConstructor
    public BlockingBuffer(final BlockingBufferConfig blockingBufferConfig, final PipelineDescription pipelineDescription) {
        this(checkNotNull(blockingBufferConfig, "BlockingBufferConfig cannot be null").getBufferSize(),
                blockingBufferConfig.getBatchSize(),
                pipelineDescription.getPipelineName());
    }

    public BlockingBuffer(final String pipelineName) {
        this(DEFAULT_BUFFER_CAPACITY, DEFAULT_BATCH_SIZE, pipelineName);
    }

    @Override
    public void doWrite(T record, int timeoutInMillis) throws TimeoutException {
        try {
            final boolean permitAcquired = capacitySemaphore.tryAcquire(timeoutInMillis, TimeUnit.MILLISECONDS);
            if (!permitAcquired) {
                throw new TimeoutException(format("Pipeline [%s] - Buffer is full, timed out waiting for a slot",
                        pipelineName));
            }
            blockingQueue.offer(record);
        } catch (InterruptedException ex) {
            LOG.error("Pipeline [{}] - Buffer is full, interrupted while waiting to write the record", pipelineName, ex);
            throw new TimeoutException("Buffer is full, timed out waiting for a slot");
        }
    }

    @Override
    public void doWriteAll(Collection<T> records, int timeoutInMillis) throws Exception {
        final int size = records.size();
        if (size > bufferCapacity) {
            throw new SizeOverflowException(format("Buffer capacity too small for the number of records: %d", size));
        }
        try {
            final boolean permitAcquired = capacitySemaphore.tryAcquire(size, timeoutInMillis, TimeUnit.MILLISECONDS);
            if (!permitAcquired) {
                throw new TimeoutException(
                        format("Pipeline [%s] - Buffer does not have enough capacity left for the number of records: %d, " +
                                        "timed out waiting for slots.",
                        pipelineName, size));
            }
            blockingQueue.addAll(records);
        } catch (InterruptedException ex) {
            LOG.error("Pipeline [{}] - Buffer does not have enough capacity left for the number of records: {}, " +
                            "interrupted while waiting to write the records",
                    pipelineName, size, ex);
            throw new TimeoutException(
                    format("Pipeline [%s] - Buffer does not have enough capacity left for the number of records: %d, " +
                            "timed out waiting for slots.",
                    pipelineName, size));
        }
    }

    /**
     * Retrieves and removes the batch of records from the head of the queue. The batch size is defined/determined by
     * the configuration attribute {@link #ATTRIBUTE_BATCH_SIZE} or the @param timeoutInMillis. The timeoutInMillis
     * is also used for retrieving each record
     *
     * @param timeoutInMillis how long to wait before giving up
     * @return The earliest batch of records in the buffer which are still not read.
     */
    @Override
    public Map.Entry<Collection<T>, CheckpointState> doRead(int timeoutInMillis) {
        final List<T> records = new ArrayList<>(batchSize);
        int recordsRead = 0;

        if (timeoutInMillis == 0) {
            final T record = pollForBufferEntry(5, TimeUnit.MILLISECONDS);
            if (record != null) { //record can be null, avoiding adding nulls
                records.add(record);
                recordsRead++;
            }

            recordsRead += blockingQueue.drainTo(records, batchSize - 1);
        } else {
            final Stopwatch stopwatch = Stopwatch.createStarted();
            while (stopwatch.elapsed(TimeUnit.MILLISECONDS) < timeoutInMillis && records.size() < batchSize) {
                final T record = pollForBufferEntry(timeoutInMillis, TimeUnit.MILLISECONDS);
                if (record != null) { //record can be null, avoiding adding nulls
                    records.add(record);
                    recordsRead++;
                }

                if (recordsRead < batchSize) {
                    recordsRead += blockingQueue.drainTo(records, batchSize - recordsRead);
                }
            }
        }

        updateLatency((Collection<T>)records);
        final CheckpointState checkpointState = new CheckpointState(recordsRead);
        return new AbstractMap.SimpleEntry<>(records, checkpointState);
    }

    private T pollForBufferEntry(final int timeoutValue, final TimeUnit timeoutUnit) {
        try {
            return blockingQueue.poll(timeoutValue, timeoutUnit);
        } catch (InterruptedException e) {
            LOG.info("Pipeline [{}] - Interrupt received while reading from buffer", pipelineName);
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns the default PluginSetting object with default values.
     * @return PluginSetting
     */
    public static PluginSetting getDefaultPluginSettings() {
        final Map<String, Object> settings = new HashMap<>();
        settings.put(ATTRIBUTE_BUFFER_CAPACITY, DEFAULT_BUFFER_CAPACITY);
        settings.put(ATTRIBUTE_BATCH_SIZE, DEFAULT_BATCH_SIZE);
        return new PluginSetting(PLUGIN_NAME, settings);
    }

    @Override
    public void doCheckpoint(final CheckpointState checkpointState) {
        final int numCheckedRecords = checkpointState.getNumRecordsToBeChecked();
        capacitySemaphore.release(numCheckedRecords);
    }

    @Override
    public boolean isEmpty() {
        return blockingQueue.isEmpty() && getRecordsInFlight() == 0;
    }
}
