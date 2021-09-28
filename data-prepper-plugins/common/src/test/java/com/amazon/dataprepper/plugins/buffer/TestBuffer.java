/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.buffer;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.CheckpointState;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.TimeoutException;

@DataPrepperPlugin(name = "test_buffer", type = PluginType.BUFFER)
public class TestBuffer implements Buffer<Record<String>> {
    private static final String ATTRIBUTE_BATCH_SIZE = "batch_size";
    private static final String ATTRIBUTE_IMITATE_TIMEOUT = "imitate_timeout";
    private static final int DEFAULT_BATCH_SIZE = 8;
    private static final boolean DEFAULT_IMITATE_TIMEOUT = false;

    private Queue<Record<String>> buffer;
    private int batchSize;
    private boolean imitateTimeout;


    public TestBuffer(final PluginSetting pluginSetting) {
        this(new LinkedList<>(),
                pluginSetting.getIntegerOrDefault(ATTRIBUTE_BATCH_SIZE, DEFAULT_BATCH_SIZE),
                pluginSetting.getBooleanOrDefault(ATTRIBUTE_IMITATE_TIMEOUT, DEFAULT_IMITATE_TIMEOUT));
    }

    public TestBuffer(final Queue<Record<String>> buffer, final int batchSize) {
        this.buffer = buffer;
        this.batchSize = batchSize;
        this.imitateTimeout = false;
    }

    public TestBuffer(final Queue<Record<String>> buffer, final int batchSize, final boolean imitateTimeout) {
        this.buffer = buffer;
        this.batchSize = batchSize;
        this.imitateTimeout = imitateTimeout;
    }


    @Override
    public void write(Record<String> record, int timeoutInMillis) throws TimeoutException {
        if (imitateTimeout) {
            throw new TimeoutException();
        }
        buffer.add(record);
    }

    @Override
    public void writeAll(final Collection<Record<String>> records, final int timeoutInMillis) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public Map.Entry<Collection<Record<String>>, CheckpointState> read(int timeoutInMillis) {
        final List<Record<String>> records = new ArrayList<>();
        int index = 0;
        Record<String> record;
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
}
