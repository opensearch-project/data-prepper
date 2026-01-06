/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.plugin;

/**
 * An interface to be implemented by pipeline plugins, i.e. {@link org.opensearch.dataprepper.model.source.Source}
 * /{@link org.opensearch.dataprepper.model.processor.Processor}/{@link org.opensearch.dataprepper.model.sink.Sink},
 * to refresh their components, e.g. client connection.
 * @since 2.5
 */
public interface PluginComponentRefresher<PluginComponent, PluginConfig> {
    /**
     * Returns the {@link PluginComponent} class.
     *
     * @return {@link PluginComponent} class.
     */
    Class<PluginComponent> getComponentClass();

    /**
     * Returns the refreshed {@link PluginComponent}.
     *
     * @return An instance of {@link PluginComponent}.
     */
    PluginComponent get();

    /**
     * Updates the {@link PluginComponent} with the new {@link PluginConfig}.
     *
     * @param pluginConfig The new pluginConfig used to refresh the {@link PluginComponent}.
     */
    void update(PluginConfig pluginConfig);
}
