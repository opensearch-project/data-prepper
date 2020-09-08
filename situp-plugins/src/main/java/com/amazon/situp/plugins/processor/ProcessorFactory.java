package com.amazon.situp.plugins.processor;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.plugins.PluginFactory;
import com.amazon.situp.plugins.PluginRepository;
import com.amazon.situp.model.processor.Processor;

@SuppressWarnings({"rawtypes"})
public class ProcessorFactory extends PluginFactory {

    public static Processor newProcessor(final PluginSetting pluginSetting) {
        return (Processor) newPlugin(pluginSetting, PluginRepository.getProcessorClass(pluginSetting.getName()));
    }
}
