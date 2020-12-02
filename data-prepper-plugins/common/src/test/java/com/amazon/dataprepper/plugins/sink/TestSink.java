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

    public List<Record<String>> getCollectedRecords() {
        return collectedRecords;
    }
}
