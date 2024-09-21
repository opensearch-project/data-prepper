/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.test;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.time.Instant;

@DataPrepperPlugin(name = "test_sink", alternateNames = { "test_sink_alternate_name" }, deprecatedName = "test_sink_deprecated_name", pluginType = Sink.class)
public class TestSink implements Sink<Record<String>> {
    public boolean isShutdown = false;
    private final List<Record<String>> collectedRecords;
    private final boolean failSinkForTest;
    private boolean ready;
    private Instant readyTime;

    public TestSink() {
        this.failSinkForTest = false;
        this.collectedRecords = new ArrayList<>();
        this.ready = true;
    }

    public TestSink(Duration readyAfter) {
        this.ready = false;
        this.failSinkForTest = false;
        this.collectedRecords = new ArrayList<>();
        this.readyTime = Instant.now().plus(readyAfter);
    }

    public TestSink(boolean failSinkForTest) {
        this.failSinkForTest = failSinkForTest;
        this.collectedRecords = new ArrayList<>();
        this.ready = false;
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
        if (!ready) {
            ready = Instant.now().isAfter(readyTime);
        }
        return ready;
    }

    public List<Record<String>> getCollectedRecords() {
        return collectedRecords;
    }
}
