package com.amazon.situp.plugins.sink;

import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.record.Record;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.sink.Sink;

import java.util.Collection;
import java.util.Iterator;

@SitupPlugin(name = "stdout", type = PluginType.SINK)
public class StdOutSink implements Sink<Record<String>> {

    /**
     * Mandatory constructor for SITUP Component - This constructor is used by SITUP
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
    public boolean output(final Collection<Record<String>> records) {
        final Iterator<Record<String>> iterator = records.iterator();
        while (iterator.hasNext()) {
            final Record<String> record = iterator.next();
            System.out.println(record.getData());
        }
        return true;
    }
}
