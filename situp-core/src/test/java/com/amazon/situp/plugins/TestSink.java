package com.amazon.situp.plugins;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.sink.Sink;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

@SitupPlugin(name = "test_sink", type = PluginType.SINK)
public class TestSink implements Sink<Record<String>> {
    private final List<Record<String>> collectedRecords;
    private final boolean failSinkForTest;

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

    public List<Record<String>> getCollectedRecords() {
        return collectedRecords;
    }
}
