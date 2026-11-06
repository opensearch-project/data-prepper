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
import org.opensearch.dataprepper.model.plugin.PluginFactory;

import javax.inject.Named;

@Named
public class PluginConfigurationObservableFactory {
    public final PluginConfigObservable createDefaultPluginConfigObservable(
            final PluginConfigurationConverter pluginConfigurationConverter, final Class<?> pluginConfigClass,
            final PluginSetting rawPluginSettings, final PluginFactory pluginFactory) {
        return new DefaultPluginConfigObservable(
                pluginConfigurationConverter, pluginConfigClass, rawPluginSettings, pluginFactory);
    }
}
