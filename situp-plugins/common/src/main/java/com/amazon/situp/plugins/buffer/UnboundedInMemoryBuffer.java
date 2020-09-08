package com.amazon.situp.plugins.buffer;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.buffer.Buffer;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.record.Record;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

/**
 * Implementation of {@link Buffer} - An unbounded in-memory FIFO buffer. The bufferSize determines the size of the
 * collection for {@link #readBatch()}.
 *
 * @param <T> a sub-class of {@link Record}
 */
@SitupPlugin(name = "unbounded-inmemory", type = PluginType.BUFFER)
public class UnboundedInMemoryBuffer<T extends Record<?>> implements Buffer<T> {
    private static final int DEFAULT_BATCH_SIZE = 8;

    private final Queue<T> queue;
    private final int batchSize;

    public UnboundedInMemoryBuffer() {
        this.queue = new LinkedList<>();
        batchSize = DEFAULT_BATCH_SIZE;
    }

    /**
     * Constructs an unbounded in-memory buffer with provided batch size. The batch size determines the size of the
     * collection for {@link #readBatch()}.
     *
     * @param batchSize the collection size for {@link #readBatch()}.
     */
    public UnboundedInMemoryBuffer(int batchSize) {
        this.queue = new LinkedList<>();
        this.batchSize = batchSize;
    }

    /**
     * Mandatory constructor for SITUP Component - This constructor is used by SITUP
     * runtime engine to construct an instance of {@link UnboundedInMemoryBuffer} using an instance of
     * {@link PluginSetting} which has access to pluginSetting metadata from pipeline pluginSetting file.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public UnboundedInMemoryBuffer(final PluginSetting pluginSetting) {
        this(getAttributeOrDefault("batch-size", pluginSetting));
    }

    @Override
    public void write(final T record) {
        //throws runtime exception if buffer is full
        queue.add(record);
    }

    @Override
    public T read() {
        //returns null if the buffer is empty
        return queue.poll();
    }

    /**
     * @return Collection of records, the maximum size of the collection is determined by the bufferSize (with default
     * value as @link{{@link #DEFAULT_BATCH_SIZE}}).
     */
    @Override
    public Collection<T> readBatch() {
        final List<T> records = new ArrayList<>();
        int index = 0;
        T record;
        while (index < batchSize && (record = this.read()) != null) {
            records.add(record);
            index++;
        }
        return records;
    }

    @Override
    public void writeBatch(Collection<T> records) {
        queue.addAll(records);
    }

    private static Integer getAttributeOrDefault(final String attribute, final PluginSetting pluginSetting) {
        final Object attributeObject = pluginSetting.getAttributeFromSettings(attribute);
        return attributeObject == null ? DEFAULT_BATCH_SIZE : (Integer) attributeObject;
    }
}
