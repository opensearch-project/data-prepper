/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.plugin.PluginConfigObserver;
import org.opensearch.dataprepper.model.plugin.PluginFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class DefaultPluginConfigObservable implements PluginConfigObservable {
    private final Map<PluginConfigObserver, Boolean> pluginConfigObserverBooleanMap
            = new ConcurrentHashMap<>();
    private final PluginConfigurationConverter pluginConfigurationConverter;
    private final Class<?> pluginConfigClass;
    private final PluginSetting rawPluginSettings;
    private final PluginFactory pluginFactory;

    DefaultPluginConfigObservable(final PluginConfigurationConverter pluginConfigurationConverter,
                                  final Class<?> pluginConfigClass,
                                  final PluginSetting rawPluginSettings,
                                  final PluginFactory pluginFactory) {
        this.pluginConfigurationConverter = pluginConfigurationConverter;
        this.pluginConfigClass = pluginConfigClass;
        this.rawPluginSettings = rawPluginSettings;
        this.pluginFactory = pluginFactory;
    }

    @Override
    public boolean addPluginConfigObserver(final PluginConfigObserver pluginConfigObserver) {
        pluginConfigObserverBooleanMap.put(pluginConfigObserver, true);
        return true;
    }

    @Override
    public void update() {
        final Object newPluginConfiguration = pluginConfigurationConverter.convert(
                pluginConfigClass, rawPluginSettings, pluginFactory);
        pluginConfigObserverBooleanMap.keySet().forEach(
                pluginConfigObserver -> pluginConfigObserver.update(newPluginConfiguration));
    }
}
