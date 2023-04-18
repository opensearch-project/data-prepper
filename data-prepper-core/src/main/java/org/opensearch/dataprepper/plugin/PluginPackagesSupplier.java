/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import com.google.common.collect.Iterators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Supplies packages to scan for plugins.
 */
class PluginPackagesSupplier implements Supplier<String[]> {

    private static final Logger LOG = LoggerFactory.getLogger(PluginPackagesSupplier.class);
    private static final String DEFAULT_PLUGINS_CLASSPATH = "org.opensearch.dataprepper.plugins";
    // TODO: Remove this once all plugins have migrated to org.opensearch
    private static final String DEPRECATED_DEFAULT_PLUGINS_CLASSPATH = "com.amazon.dataprepper.plugins";

    PluginPackagesSupplier() {
    }

    Iterator<URL> loadResources() throws IOException {
        final Enumeration<URL> resourcesEnumeration = getClass().getClassLoader().getResources("META-INF/data-prepper.plugins.properties");
        return Iterators.forEnumeration(resourcesEnumeration);
    }

    @Override
    public String[] get() {

        final Set<String> packageNames = getUniquePackageNames();

        final String[] packageNamesArray = packageNames.toArray(new String[0]);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Using packages for plugins: {}", String.join(",", packageNamesArray));
        }

        return packageNamesArray;
    }

    private Set<String> getUniquePackageNames() {
        Set<String> packageNames;
        try {
            final Iterator<URL> pluginResources = loadResources();
            packageNames = readResources(pluginResources);
        } catch (final IOException ex) {
            LOG.warn("Unable to load data-prepper.plugins.properties file. Reverting to default plugin package.");
            packageNames = new LinkedHashSet<>(Arrays.asList(DEFAULT_PLUGINS_CLASSPATH, DEPRECATED_DEFAULT_PLUGINS_CLASSPATH));
        }
        return packageNames;
    }

    private Set<String> readResources(final Iterator<URL> pluginResources) {
        final Set<String> packageNames = new HashSet<>();
        while (pluginResources.hasNext()) {

            final URL pluginPropertiesUrl = pluginResources.next();

            final Properties pluginProperties = new Properties();
            try {
                final InputStream pluginPropertiesInputStream = pluginPropertiesUrl.openStream();
                pluginProperties.load(pluginPropertiesInputStream);
            } catch (final IOException ex) {
                LOG.warn("Unable to load properties from resource. url={}", pluginPropertiesUrl, ex);
            }

            final String packagesRawString = pluginProperties.getProperty("org.opensearch.dataprepper.plugin.packages", "");

            final String[] currentPackageNames = packagesRawString.split(",");

            final List<String> validPackageNames = Arrays.stream(currentPackageNames)
                    .filter(p -> !p.isEmpty())
                    .collect(Collectors.toList());
            packageNames.addAll(validPackageNames);
        }

        if (packageNames.isEmpty()) {
            LOG.warn("Unable to load packages from data-prepper.plugins.properties file. Reverting to default plugin package.");
            packageNames.add(DEFAULT_PLUGINS_CLASSPATH);
            packageNames.add(DEPRECATED_DEFAULT_PLUGINS_CLASSPATH);
        }

        return packageNames;
    }
}
