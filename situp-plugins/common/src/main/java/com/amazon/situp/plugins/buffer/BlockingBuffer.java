package com.amazon.situp.plugins.buffer;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * A bounded BlockingBuffer is an implementation of {@link Buffer} using {@link LinkedBlockingQueue}, it is bounded to
 * the provided capacity {@link #ATTRIBUTE_BUFFER_CAPACITY} or {@link #DEFAULT_BUFFER_CAPACITY} if attribute is not
 * provided; {@link #write(Record)} inserts specified non-null record into this buffer, waiting up to the specified
 * {@link #ATTRIBUTE_TIMEOUT} milliseconds if necessary for space to become available; and throws an exception if the
 * record is null.
 * @param <T> a sub-class of {@link Record}
 */
@SitupPlugin(name = "bounded-blocking", type = PluginType.BUFFER)
public class BlockingBuffer<T extends Record<?>> implements Buffer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(BlockingBuffer.class);

    private static final int DEFAULT_BATCH_SIZE = 8;
    private static final int DEFAULT_TIMEOUT = 3_000;
    private static final int DEFAULT_BUFFER_CAPACITY = 512;
    private static final String ATTRIBUTE_BATCH_SIZE = "batch-size";
    private static final String ATTRIBUTE_BUFFER_CAPACITY = "buffer-size";
    private static final String ATTRIBUTE_TIMEOUT = "timeout";

    private final int bufferCapacity;
    private final int timeout;
    private final int batchSize;
    private final BlockingQueue<T> blockingQueue;

    /**
     * Creates a BlockingBuffer with the given (fixed) capacity. Uses the given timeout for
     * @param bufferCapacity the capacity of the buffer
     * @param timeout the timeout for writing
     * @param batchSize the batch size for {@link #readBatch()}
     */
    public BlockingBuffer(final int bufferCapacity, final int timeout, final int batchSize) {
        this.bufferCapacity = bufferCapacity;
        this.timeout = timeout;
        this.batchSize = batchSize;
        this.blockingQueue = new LinkedBlockingQueue<>(bufferCapacity);
    }

    /**
     * Mandatory constructor for SITUP Component - This constructor is used by SITUP
     * runtime engine to construct an instance of {@link BlockingBuffer} using an instance of
     * {@link PluginSetting} which has access to pluginSetting metadata from pipeline pluginSetting file.
     * Buffer settings like `buffer-size`, `timeout`, `batch-size` are optional and can be passed via
     * {@link PluginSetting}, if not present default values will be used to create the buffer.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public BlockingBuffer(final PluginSetting pluginSetting) {
        this(getBufferCapacityFromSettings(pluginSetting), getWriteTimeoutFromSettings(pluginSetting),
                getBatchSizeFromSettings(pluginSetting));
    }

    @Override
    public void write(T record) {
        try {
            blockingQueue.offer(record, timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            LOG.error("Buffer is full, interrupted while waiting to write the record", ex);
        }
    }

    @Override
    public T read() {
        try {
            return blockingQueue.poll(timeout, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            LOG.error("Buffer is empty, interrupted while waiting for the record", ex);
            return null;
        }
    }

    @Override
    public Collection<T> readBatch() {
        final List<T> records = new ArrayList<>();
        try{
            blockingQueue.drainTo(records, batchSize);
        } catch(Exception ex) {
            LOG.error("Encountered exception retrieving records from buffer, continuing..", ex);
        }
        return records;
    }

    private static int getBufferCapacityFromSettings(final PluginSetting pluginSetting) {
        return getAttributeOrDefault(ATTRIBUTE_BUFFER_CAPACITY, pluginSetting, DEFAULT_BUFFER_CAPACITY);
    }

    private static int getWriteTimeoutFromSettings(final PluginSetting pluginSetting) {
        return getAttributeOrDefault(ATTRIBUTE_TIMEOUT, pluginSetting, DEFAULT_TIMEOUT);
    }

    private static int getBatchSizeFromSettings(final PluginSetting pluginSetting) {
        return getAttributeOrDefault(ATTRIBUTE_BATCH_SIZE, pluginSetting, DEFAULT_BATCH_SIZE);
    }

    private static Integer getAttributeOrDefault(final String attribute, final PluginSetting pluginSetting, int defaultValue) {
        final Object attributeObject = pluginSetting.getAttributeFromSettings(attribute);
        return attributeObject == null ? defaultValue : (Integer) attributeObject;
    }
}
