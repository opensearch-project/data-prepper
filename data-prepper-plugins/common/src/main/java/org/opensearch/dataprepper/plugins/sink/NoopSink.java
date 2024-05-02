package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;

import java.util.Collection;

@DataPrepperPlugin(name = "noop", pluginType = Sink.class)
public class NoopSink implements Sink<Record<Object>> {
    @Override
    public void output(Collection<Record<Object>> records) {
        // empty by design.
    }

    @Override
    public Object outputSync(Collection<Record<Object>> records, boolean isQuery) {

        return null;
    }

    @Override
    public void shutdown() {
        // empty by design.
    }

    @Override
    public void initialize() {
        // empty by design.
    }

    @Override
    public boolean isReady() {
        return true;
    }
}
