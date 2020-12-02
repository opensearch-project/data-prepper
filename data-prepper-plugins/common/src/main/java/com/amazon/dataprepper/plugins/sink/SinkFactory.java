package com.amazon.dataprepper.plugins.sink;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.plugins.PluginFactory;
import com.amazon.dataprepper.plugins.PluginRepository;

@SuppressWarnings({"rawtypes"})
public class SinkFactory extends PluginFactory {

    public static Sink newSink(final PluginSetting pluginSetting) {
        return (Sink) newPlugin(pluginSetting, PluginRepository.getSinkClass(pluginSetting.getName()));
    }
}
