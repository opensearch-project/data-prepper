package com.amazon.situp.plugins.sink;

import com.amazon.situp.model.configuration.PluginSetting;
import com.amazon.situp.plugins.PluginFactory;
import com.amazon.situp.plugins.PluginRepository;
import com.amazon.situp.model.sink.Sink;

@SuppressWarnings({"rawtypes"})
public class SinkFactory extends PluginFactory {

    public static Sink newSink(final PluginSetting pluginSetting) {
        return (Sink) newPlugin(pluginSetting, PluginRepository.getSinkClass(pluginSetting.getName()));
    }
}
