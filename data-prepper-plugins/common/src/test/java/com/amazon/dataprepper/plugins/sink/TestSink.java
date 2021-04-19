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

package com.amazon.dataprepper.plugins.sink;


import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.sink.Sink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@DataPrepperPlugin(name = "test-sink", type = PluginType.SINK)
public class TestSink implements Sink<Record<String>> {
    private final List<Record<String>> collectedRecords;

    public TestSink() {
        collectedRecords = new ArrayList<>();
    }

    @Override
    public void output(Collection<Record<String>> records) {
        records.stream().collect(Collectors.toCollection(() -> collectedRecords));
    }

    @Override
    public void shutdown() {

    }

    public List<Record<String>> getCollectedRecords() {
        return collectedRecords;
    }
}
