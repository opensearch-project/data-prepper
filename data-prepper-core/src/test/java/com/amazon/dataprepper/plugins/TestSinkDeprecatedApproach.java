package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.sink.Sink;

import java.util.Collection;

@DataPrepperPlugin(name = "test_sink_deprecated_type", type = PluginType.SINK)
public class TestSinkDeprecatedApproach implements Sink<Record<String>> {
    @Override
    public void output(final Collection<Record<String>> records) {

    }

    @Override
    public void shutdown() {

    }
}
