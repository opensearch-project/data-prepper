package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.PluginFactory;
import com.amazon.dataprepper.plugins.PluginRepository;
import com.amazon.dataprepper.model.source.Source;

@SuppressWarnings({"rawtypes"})
public class SourceFactory extends PluginFactory {

    public static Source newSource(final PluginSetting pluginSetting) {
        return (Source) newPlugin(pluginSetting, PluginRepository.getSourceClass(pluginSetting.getName()));
    }
}