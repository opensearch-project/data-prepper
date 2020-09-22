package com.amazon.situp.plugins.buffer;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A bounded BlockingBuffer is an implementation of {@link Buffer} using {@link LinkedBlockingQueue}, it is bounded
 * to the provided capacity {@link #ATTRIBUTE_BUFFER_CAPACITY} or {@link #DEFAULT_BUFFER_CAPACITY} (if attribute is
 * not provided); {@link #write(Record, int)} inserts specified non-null record into this buffer, waiting up to the
 * specified timeout in milliseconds if necessary for space to become available; and throws an exception if the
 * record is null. {@link #read(int)} retrieves and removes the batch of records from the head of the queue. The
 * batch size is defined/determined by the configuration attribute {@link #ATTRIBUTE_BATCH_SIZE} or the timeout parameter
 */
@SitupPlugin(name = "bounded_blocking", type = PluginType.BUFFER)
public class BlockingBuffer<T extends Record<?>> implements Buffer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(BlockingBuffer.class);
    private static final int DEFAULT_BUFFER_CAPACITY = 512;
    private static final int DEFAULT_BATCH_SIZE = 8;
    private static final String ATTRIBUTE_BUFFER_CAPACITY = "buffer_size";
    private static final String ATTRIBUTE_BATCH_SIZE = "batch_size";

    private final int batchSize;
    private final BlockingQueue<T> blockingQueue;

    /**
     * Creates a BlockingBuffer with the given (fixed) capacity.
     *
     * @param bufferCapacity the capacity of the buffer
     * @param batchSize      the batch size for {@link #read(int)}
     */
    public BlockingBuffer(final int bufferCapacity, final int batchSize) {
        this.batchSize = batchSize;
        this.blockingQueue = new LinkedBlockingQueue<>(bufferCapacity);
    }

    /**
     * Mandatory constructor for SITUP Component - This constructor is used by SITUP runtime engine to construct an
     * instance of {@link BlockingBuffer} using an instance of {@link PluginSetting} which has access to
     * pluginSetting metadata from pipeline pluginSetting file. Buffer settings like `buffer-size`, `batch-size`,
     * `batch-timeout` are optional and can be passed via {@link PluginSetting}, if not present default values will
     * be used to create the buffer.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public BlockingBuffer(final PluginSetting pluginSetting) {
        this(pluginSetting.getIntegerOrDefault(ATTRIBUTE_BUFFER_CAPACITY, DEFAULT_BUFFER_CAPACITY),
                pluginSetting.getIntegerOrDefault(ATTRIBUTE_BATCH_SIZE, DEFAULT_BATCH_SIZE));
    }

    public BlockingBuffer() {
        this(DEFAULT_BUFFER_CAPACITY, DEFAULT_BATCH_SIZE);
    }

    @Override
    public void write(T record, int timeoutInMillis) throws TimeoutException {
        try {
            boolean isSuccess = blockingQueue.offer(record, timeoutInMillis, TimeUnit.MILLISECONDS);
            if (!isSuccess) {
                throw new TimeoutException("Buffer is full, timed out waiting for a slot");
            }
        } catch (InterruptedException ex) {
            LOG.error("Buffer is full, interrupted while waiting to write the record", ex);
            throw new TimeoutException("Buffer is full, timed out waiting for a slot");
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
    public Collection<T> read(int timeoutInMillis) {
        final List<T> records = new ArrayList<>();
        final Stopwatch stopwatch = Stopwatch.createStarted();
        try {
            while (stopwatch.elapsed(TimeUnit.MILLISECONDS) < timeoutInMillis && records.size() < batchSize) {
                final T record = blockingQueue.poll(timeoutInMillis, TimeUnit.MILLISECONDS);
                if (record != null) { //record can be null, avoiding adding nulls
                    records.add(record);
                }
                if (records.size() < batchSize) {
                    blockingQueue.drainTo(records, batchSize - records.size());
                }
            }
        } catch (InterruptedException ex) {
            LOG.warn("Retrieving records from buffer to batch size timed out, returning already retrieved records", ex);
            return records;
        }
        return records;
    }
}
