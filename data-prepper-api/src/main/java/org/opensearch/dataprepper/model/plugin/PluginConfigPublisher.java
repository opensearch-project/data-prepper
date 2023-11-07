package org.opensearch.dataprepper.model.plugin;

/**
 * An interface used to onboard and notify all {@link PluginConfigObservable} to invoke update.
 * @since 2.5
 */
public interface PluginConfigPublisher {
    /**
     * Onboard a new {@link PluginConfigObservable}.
     *
     * @param pluginConfigObservable observable plugin configuration
     * @return true if the operation is successful, false otherwise
     */
    boolean addPluginConfigObservable(PluginConfigObservable pluginConfigObservable);

    /**
     * Notify all {@link PluginConfigObservable} to update.
     */
    void notifyAllPluginConfigObservable();
}
