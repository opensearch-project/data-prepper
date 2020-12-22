package com.amazon.dataprepper.plugins.sink;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.record.Record;
import com.amazon.dataprepper.model.sink.Sink;

import java.util.Collection;
import java.util.Iterator;

@DataPrepperPlugin(name = "stdout", type = PluginType.SINK)
public class StdOutSink implements Sink<Record<String>> {

    /**
     * Mandatory constructor for Data Prepper Component - This constructor is used by Data Prepper
     * runtime engine to construct an instance of {@link StdOutSink} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public StdOutSink(final PluginSetting pluginSetting) {
        this();
    }

    public StdOutSink() {
    }

    @Override
    public void output(final Collection<Record<String>> records) {
        final Iterator<Record<String>> iterator = records.iterator();
        while (iterator.hasNext()) {
            final Record<String> record = iterator.next();
            System.out.println(record.getData());
        }
    }

    @Override
    public void shutdown() {

    }
}
