package com.amazon.dataprepper.plugins.processor;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.plugins.PluginFactory;
import com.amazon.dataprepper.plugins.PluginRepository;

@SuppressWarnings({"rawtypes"})
public class ProcessorFactory extends PluginFactory {

    public static Processor newProcessor(final PluginSetting pluginSetting) {
        return (Processor) newPlugin(pluginSetting, PluginRepository.getProcessorClass(pluginSetting.getName()));
    }
}
