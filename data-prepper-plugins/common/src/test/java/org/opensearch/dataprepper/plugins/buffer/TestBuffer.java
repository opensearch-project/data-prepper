/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.buffer;

import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.time.Duration;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeoutException;

@DataPrepperPlugin(name = "test_buffer", pluginType = Buffer.class)
public class TestBuffer implements Buffer<Record<Event>> {
    private static final String ATTRIBUTE_BATCH_SIZE = "batch_size";
    private static final String ATTRIBUTE_IMITATE_TIMEOUT = "imitate_timeout";
    private static final int DEFAULT_BATCH_SIZE = 8;
    private static final boolean DEFAULT_IMITATE_TIMEOUT = false;

    private Queue<Record<Event>> buffer;
    private int batchSize;
    private boolean imitateTimeout;


    public TestBuffer(final PluginSetting pluginSetting) {
        this(new LinkedList<>(),
                pluginSetting.getIntegerOrDefault(ATTRIBUTE_BATCH_SIZE, DEFAULT_BATCH_SIZE),
                pluginSetting.getBooleanOrDefault(ATTRIBUTE_IMITATE_TIMEOUT, DEFAULT_IMITATE_TIMEOUT));
    }

    public TestBuffer(final Queue<Record<Event>> buffer, final int batchSize) {
        this.buffer = buffer;
        this.batchSize = batchSize;
        this.imitateTimeout = false;
    }

    public TestBuffer(final Queue<Record<Event>> buffer, final int batchSize, final boolean imitateTimeout) {
        this.buffer = buffer;
        this.batchSize = batchSize;
        this.imitateTimeout = imitateTimeout;
    }


    @Override
    public void write(Record<Event> record, int timeoutInMillis) throws TimeoutException {
        if (imitateTimeout) {
            throw new TimeoutException();
        }
        buffer.add(record);
    }

    @Override
    public void writeAll(final Collection<Record<Event>> records, final int timeoutInMillis) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map.Entry<Collection<Record<Event>>, CheckpointState> read(int timeoutInMillis) {
        final List<Record<Event>> records = new ArrayList<>();
        int index = 0;
        Record<Event> record;
        while (index < batchSize && (record = buffer.poll()) != null) {
            records.add(record);
            index++;
        }
        final CheckpointState checkpointState = new CheckpointState(records.size());
        return new AbstractMap.SimpleEntry<>(records, checkpointState);
    }

    @Override
    public void checkpoint(final CheckpointState checkpointState) {

    }

    @Override
    public boolean isEmpty() {
        return buffer.isEmpty();
    }

    public int size() {
        return buffer.size();
    }

    @Override
    public Duration getDrainTimeout() {
        return Duration.ZERO;
    }
}
