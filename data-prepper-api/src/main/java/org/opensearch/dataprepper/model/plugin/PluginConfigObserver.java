package org.opensearch.dataprepper.model.plugin;

/**
 * An interface by pipeline plugins, i.e. {@link org.opensearch.dataprepper.model.source.Source}
 *  * /{@link org.opensearch.dataprepper.model.processor.Processor}/{@link org.opensearch.dataprepper.model.sink.Sink}
 *  to implement custom plugin component refreshment logic.
 * @since 2.5
 */
public interface PluginConfigObserver<T> {
    /**
     * Update plugin components with a new pluginConfig.
     *
     * @param pluginConfig The plugin configuration object used as reference for update.
     */
    void update(T pluginConfig);
}
