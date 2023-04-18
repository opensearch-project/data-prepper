/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@DataPrepperPlugin(name = "test_sink", deprecatedName = "test_sink_deprecated_name", pluginType = Sink.class)
public class TestSink implements Sink<Record<String>> {
    private final List<Record<String>> collectedRecords;
    private final boolean failSinkForTest;
    public boolean isShutdown = false;

    public TestSink() {
        this.failSinkForTest = false;
        this.collectedRecords = new ArrayList<>();
    }

    public TestSink(boolean failSinkForTest) {
        this.failSinkForTest = failSinkForTest;
        this.collectedRecords = new ArrayList<>();
    }

    @Override
    public void output(Collection<Record<String>> records) {
        if(failSinkForTest) {
            throw new RuntimeException("Sink is expected to fail");
        }
        records.stream().collect(Collectors.toCollection(() -> collectedRecords));
    }

    @Override
    public void shutdown() {
        isShutdown = true;
    }

    @Override
    public void initialize() {
    }

    @Override
    public boolean isReady() {
        return true;
    }

    public List<Record<String>> getCollectedRecords() {
        return collectedRecords;
    }
}
