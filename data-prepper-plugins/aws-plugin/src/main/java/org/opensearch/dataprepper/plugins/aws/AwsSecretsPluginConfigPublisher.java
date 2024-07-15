/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.aws;

import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AwsSecretsPluginConfigPublisher implements PluginConfigPublisher {
    private final Map<PluginConfigObservable, Boolean> pluginConfigurationObservableBooleanMap
            = new ConcurrentHashMap<>();

    @Override
    public boolean addPluginConfigObservable(final PluginConfigObservable pluginConfigObservable) {
        pluginConfigurationObservableBooleanMap.put(pluginConfigObservable, true);
        return true;
    }

    @Override
    public void notifyAllPluginConfigObservable() {
        pluginConfigurationObservableBooleanMap.keySet().forEach(PluginConfigObservable::update);
    }
}
