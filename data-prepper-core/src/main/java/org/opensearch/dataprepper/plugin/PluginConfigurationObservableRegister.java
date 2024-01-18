/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.plugin.PluginConfigPublisher;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;

@Named
public class PluginConfigurationObservableRegister {
    private final Set<PluginConfigPublisher> pluginConfigPublishers;

    @Inject
    public PluginConfigurationObservableRegister(final Set<PluginConfigPublisher> pluginConfigPublishers) {
        this.pluginConfigPublishers = pluginConfigPublishers;
    }

    public void registerPluginConfigurationObservables(final Object[] constructorArguments) {
        Optional.ofNullable(constructorArguments).ifPresent(arguments -> Arrays.stream(arguments)
                .filter(arg -> arg instanceof PluginConfigObservable)
                .forEach(arg -> pluginConfigPublishers.forEach(pluginConfigPublisher ->
                        pluginConfigPublisher.addPluginConfigObservable(
                                (PluginConfigObservable) arg)))
        );
    }
}
