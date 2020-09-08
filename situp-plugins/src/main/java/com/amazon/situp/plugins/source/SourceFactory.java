package com.amazon.situp.plugins.source;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.plugins.PluginFactory;
import com.amazon.situp.plugins.PluginRepository;
import com.amazon.situp.model.source.Source;

@SuppressWarnings({"rawtypes"})
public class SourceFactory extends PluginFactory {

    public static Source newSource(final PluginSetting pluginSetting) {
        return (Source) newPlugin(pluginSetting, PluginRepository.getSourceClass(pluginSetting.getName()));
    }
}
