package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.sink.Sink;

import java.util.Collection;

@DataPrepperPlugin(name = "test_sink_updated", pluginType = Sink.class)
public class TestSinkUpdated implements Sink<Record<String>> {
    @Override
    public void output(final Collection<Record<String>> records) {

    }

    @Override
    public void shutdown() {

    }
}
