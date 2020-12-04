package com.amazon.dataprepper.plugins.buffer;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.PluginFactory;
import com.amazon.dataprepper.plugins.PluginRepository;

@SuppressWarnings({"rawtypes"})
public class BufferFactory extends PluginFactory {

    public static Buffer newBuffer(final PluginSetting pluginSetting) {
        return (Buffer) newPlugin(pluginSetting, PluginRepository.getBufferClass(pluginSetting.getName()));
    }
}
