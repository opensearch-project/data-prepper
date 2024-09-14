package org.opensearch.dataprepper.plugins.sink;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.Sink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

@DataPrepperPlugin(name = "noop", pluginType = Sink.class)
public class NoopSink implements Sink<Record<Object>> {
    private static final Logger LOG = LoggerFactory.getLogger(NoopSink.class);

    @Override
    public void output(Collection<Record<Object>> records) {
        LOG.info("Releasing events for NOOP sink");
        for (Record<Object> record : records) {
            Event event =  (Event)record.getData();
            event.getEventHandle().release(true);
        }
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
