package org.opensearch.dataprepper.model.plugin;

/**
 * An interface used by pipeline plugins, i.e. {@link org.opensearch.dataprepper.model.source.Source}
 * /{@link org.opensearch.dataprepper.model.processor.Processor}/{@link org.opensearch.dataprepper.model.sink.Sink},
 * to onboard {@link PluginConfigObserver}.
 * @since 2.5
 */
public interface PluginConfigObservable {

    /**
     * Onboard a new {@link PluginConfigObserver} within the plugin.
     */
    boolean addPluginConfigObserver(PluginConfigObserver pluginConfigObserver);

    /**
     * Invoke all {@link PluginConfigObserver}.
     */
    void update();
}
