package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.plugins.PluginFactory;
import com.amazon.dataprepper.plugins.PluginRepository;

@SuppressWarnings({"rawtypes"})
public class PrepperFactory extends PluginFactory {

    public static Prepper newPrepper(final PluginSetting pluginSetting) {
        return (Prepper) newPlugin(pluginSetting, PluginRepository.getPrepperClass(pluginSetting.getName()));
    }
}
