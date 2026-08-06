/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugin;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Loads and provides plugin providers via SPI and optional runtime registration.
 * Made public solely for the OSGi integration module to register additional providers.
 */
@Named
public class PluginProviderLoader {
    private static final Logger LOG = LoggerFactory.getLogger(PluginProviderLoader.class);
    private static final String PLUGIN_FRAMEWORK_PROPERTY = "data-prepper.plugin.framework";
    private static final String MODE_OSGI = "osgi";
    private final List<PluginProvider> classpathProviders;
    private final List<PluginProvider> additionalProviders = new CopyOnWriteArrayList<>();
    private final String frameworkMode;

    PluginProviderLoader() {
        final ServiceLoader<PluginProvider> serviceLoader = ServiceLoader.load(PluginProvider.class);
        classpathProviders = StreamSupport.stream(serviceLoader.spliterator(), false)
                .collect(Collectors.toList());
        frameworkMode = System.getProperty(PLUGIN_FRAMEWORK_PROPERTY, "legacy");
    }

    /**
     * Registers an additional PluginProvider at runtime.
     * Used by the OSGi integration to inject the OsgiPluginRegistry.
     *
     * @param provider the provider to add
     */
    public void registerProvider(final PluginProvider provider) {
        additionalProviders.add(provider);
        LOG.info("Registered additional PluginProvider: {}", provider.getClass().getSimpleName());
    }

    Collection<PluginProvider> getPluginProviders() {
        final List<PluginProvider> pluginProviders;

        if (MODE_OSGI.equalsIgnoreCase(frameworkMode) && !additionalProviders.isEmpty()) {
            pluginProviders = new ArrayList<>(additionalProviders);
            pluginProviders.addAll(classpathProviders);
            LOG.debug("Plugin framework running in OSGi mode with {} providers ({} OSGi + {} classpath)",
                    pluginProviders.size(), additionalProviders.size(), classpathProviders.size());
        } else {
            pluginProviders = new ArrayList<>(classpathProviders);
        }

        LOG.debug("Data Prepper is configured with {} distinct plugin providers.", pluginProviders.size());

        return pluginProviders;
    }
}
