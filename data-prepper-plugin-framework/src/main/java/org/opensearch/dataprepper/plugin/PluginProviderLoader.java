/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Named
class PluginProviderLoader {
    private static final Logger LOG = LoggerFactory.getLogger(PluginProviderLoader.class);
    private final ServiceLoader<PluginProvider> serviceLoader;

    PluginProviderLoader() {
        serviceLoader = ServiceLoader.load(PluginProvider.class);
    }

    Collection<PluginProvider> getPluginProviders() {
        final List<PluginProvider> pluginProviders = StreamSupport.stream(serviceLoader.spliterator(), false)
                .collect(Collectors.toList());

        LOG.debug("Data Prepper is configured with {} distinct plugin providers.", pluginProviders.size());

        return pluginProviders;
    }
}
