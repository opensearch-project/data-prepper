package com.amazon.situp;


import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.configuration.Configuration;
import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.sink.Sink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@SitupPlugin(name = "test-sink", type = PluginType.SINK)
public class TestSink implements Sink<Record<String>> {
    private final List<Record<String>> collectedRecords;

    public TestSink(final Configuration configuration) {
        this();
    }

    public TestSink() {
        collectedRecords = new ArrayList<>();
    }

    @Override
    public boolean output(Collection<Record<String>> records) {
        records.stream().collect(Collectors.toCollection(() -> collectedRecords));
        return true;
    }

    public List<Record<String>> getCollectedRecords() {
        return collectedRecords;
    }
}
