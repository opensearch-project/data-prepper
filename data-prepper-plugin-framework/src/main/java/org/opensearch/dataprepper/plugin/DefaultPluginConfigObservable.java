/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.plugin.PluginConfigObserver;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DefaultPluginConfigObservable implements PluginConfigObservable {
    private final Map<PluginConfigObserver, Boolean> pluginConfigObserverBooleanMap
            = new ConcurrentHashMap<>();
    private final PluginConfigurationConverter pluginConfigurationConverter;
    private final Class<?> pluginConfigClass;
    private final PluginSetting rawPluginSettings;

    public DefaultPluginConfigObservable(final PluginConfigurationConverter pluginConfigurationConverter,
                                         final Class<?> pluginConfigClass,
                                         final PluginSetting rawPluginSettings) {
        this.pluginConfigurationConverter = pluginConfigurationConverter;
        this.pluginConfigClass = pluginConfigClass;
        this.rawPluginSettings = rawPluginSettings;
    }

    @Override
    public boolean addPluginConfigObserver(final PluginConfigObserver pluginConfigObserver) {
        pluginConfigObserverBooleanMap.put(pluginConfigObserver, true);
        return true;
    }

    @Override
    public void update() {
        final Object newPluginConfiguration = pluginConfigurationConverter.convert(
                pluginConfigClass, rawPluginSettings);
        pluginConfigObserverBooleanMap.keySet().forEach(
                pluginConfigObserver -> pluginConfigObserver.update(newPluginConfiguration));
    }
}
