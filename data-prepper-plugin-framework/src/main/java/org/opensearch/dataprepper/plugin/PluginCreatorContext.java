package org.opensearch.dataprepper.plugin;

import org.springframework.context.annotation.Bean;

import javax.inject.Named;

@Named
public class PluginCreatorContext {
    @Bean(name = "extensionPluginCreator")
    public PluginCreator observablePluginCreator() {
        return new PluginCreator();
    }

    @Bean(name = "pluginCreator")
    public PluginCreator pluginCreator(
            final PluginConfigurationObservableRegister pluginConfigurationObservableRegister) {
        return new PluginCreator(pluginConfigurationObservableRegister);
    }
}
