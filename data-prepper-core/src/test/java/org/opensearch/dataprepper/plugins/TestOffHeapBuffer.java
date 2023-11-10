/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeoutException;

@DataPrepperPlugin(name = "test_off_heap", pluginType = Buffer.class)
public class TestOffHeapBuffer implements Buffer {
    @Override
    public void write(Record record, int timeoutInMillis) throws TimeoutException {

    }

    @Override
    public void writeAll(Collection records, int timeoutInMillis) throws Exception {

    }

    @Override
    public Map.Entry<Collection, CheckpointState> read(int timeoutInMillis) {
        return null;
    }

    @Override
    public void checkpoint(CheckpointState checkpointState) {

    }

    @Override
    public boolean isWrittenOffHeapOnly() {
        return true;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }
}
