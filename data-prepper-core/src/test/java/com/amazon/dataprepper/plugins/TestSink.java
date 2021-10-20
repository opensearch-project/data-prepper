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

package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.sink.Sink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@DataPrepperPlugin(name = "test_sink", pluginType = Sink.class)
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

    public List<Record<String>> getCollectedRecords() {
        return collectedRecords;
    }
}
