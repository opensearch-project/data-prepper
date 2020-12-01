package com.amazon.situp.plugins.processor;

import com.amazon.situp.model.PluginType;
import com.amazon.situp.model.annotations.SitupPlugin;
import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.model.processor.Processor;
import com.amazon.situp.model.record.Record;

import java.util.Collection;

@SitupPlugin(name = "no-op", type = PluginType.PROCESSOR)
public class NoOpProcessor<InputT extends Record<?>> implements Processor<InputT, InputT> {

    /**
     * Mandatory constructor for SITUP Component - This constructor is used by SITUP
     * runtime engine to construct an instance of {@link NoOpProcessor} using an instance of {@link PluginSetting} which
     * has access to pluginSetting metadata from pipeline
     * pluginSetting file.
     *
     * @param pluginSetting instance with metadata information from pipeline pluginSetting file.
     */
    public NoOpProcessor(final PluginSetting pluginSetting) {
        //no op
    }

    public NoOpProcessor() {

    }

    @Override
    public Collection<InputT> execute(Collection<InputT> records) {
        return records;
    }
}
