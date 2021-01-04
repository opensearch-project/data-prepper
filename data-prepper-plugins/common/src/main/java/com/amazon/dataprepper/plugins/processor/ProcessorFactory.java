package com.amazon.dataprepper.plugins.processor;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.plugins.PluginFactory;
import com.amazon.dataprepper.plugins.PluginRepository;

@SuppressWarnings({"rawtypes"})
public class ProcessorFactory extends PluginFactory {

    public static Prepper newProcessor(final PluginSetting pluginSetting) {
        return (Prepper) newPlugin(pluginSetting, PluginRepository.getProcessorClass(pluginSetting.getName()));
    }
}
