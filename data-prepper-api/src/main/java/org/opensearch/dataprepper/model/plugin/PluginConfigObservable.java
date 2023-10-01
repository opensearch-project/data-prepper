package org.opensearch.dataprepper.model.plugin;

import org.opensearch.dataprepper.model.configuration.PluginSetting;

/**
 * An interface used by pipeline plugins, i.e. {@link org.opensearch.dataprepper.model.source.Source}
 * /{@link org.opensearch.dataprepper.model.processor.Processor}/{@link org.opensearch.dataprepper.model.sink.Sink},
 * to onboard {@link PluginConfigSubscriber}.
 * @since 2.5
 */
public interface PluginConfigObservable {
    /**
     * Returns the plugin configuration class to construct the plugin.
     */
    Class<?> pluginConfigClass();

    /**
     * Returns the raw {@link PluginSetting} to generate the plugin configuration.
     */
    PluginSetting rawPluginSettings();

    /**
     * Onboard a new {@link PluginConfigSubscriber} within the plugin.
     */
    Boolean addPluginConfigSubscriber(PluginConfigSubscriber pluginConfigSubscriber);

    /**
     * Invoke all {@link PluginConfigSubscriber}.
     */
    void update();
}
