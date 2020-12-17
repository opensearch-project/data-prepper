package com.amazon.dataprepper.plugins.processor;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.model.record.Record;

import java.util.Collection;

@DataPrepperPlugin(name = "no-op", type = PluginType.PROCESSOR)
public class NoOpProcessor<InputT extends Record<?>> implements Processor<InputT, InputT> {

    /**
     * Mandatory constructor for Data Prepper Component - This constructor is used by Data Prepper
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

    @Override
    public void shutdown() {

    }
}
