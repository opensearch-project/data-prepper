/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugin.osgi;

import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Supports runtime bundle install/uninstall for dynamic plugin loading.
 * Each installed bundle gets its own classloader for isolation.
 * <p>
 * <strong>Test-scoped only.</strong> This class is excluded from the production
 * classpath. It exists as an experimental capability for integration tests that
 * exercise dynamic bundle lifecycle. Production hot-reload is not supported in
 * the initial OSGi release and is pending future design work.
 */
public class PluginHotLoader {
    private static final Logger LOG = LoggerFactory.getLogger(PluginHotLoader.class);

    private final BundleAdapter bundleAdapter;
    private final BundleContext bundleContext;
    private final Map<String, Bundle> installedBundles = new ConcurrentHashMap<>();
    private volatile OsgiPluginRegistry pluginRegistry;

    public PluginHotLoader(final BundleContext bundleContext, final BundleAdapter bundleAdapter) {
        this.bundleContext = Objects.requireNonNull(bundleContext, "bundleContext must not be null");
        this.bundleAdapter = Objects.requireNonNull(bundleAdapter, "bundleAdapter must not be null");
    }

    public void setPluginRegistry(final OsgiPluginRegistry pluginRegistry) {
        this.pluginRegistry = pluginRegistry;
    }

    /**
     * Installs and starts a plugin bundle from a JAR file at runtime.
     *
     * @param jarFile the plugin JAR to install
     * @return the symbolic name of the installed bundle
     * @throws IOException if the JAR cannot be read
     * @throws BundleException if the bundle cannot be installed or started
     */
    public String installPlugin(final File jarFile) throws IOException, BundleException {
        Objects.requireNonNull(jarFile, "jarFile must not be null");
        final Bundle bundle = bundleAdapter.adaptAndInstall(jarFile);
        bundle.start();
        final String symbolicName = bundle.getSymbolicName();
        installedBundles.put(symbolicName, bundle);
        if (pluginRegistry != null) {
            pluginRegistry.refresh();
        }
        LOG.info("Hot-loaded plugin bundle: {} (id={})", symbolicName, bundle.getBundleId());
        return symbolicName;
    }

    /**
     * Uninstalls a previously installed plugin bundle.
     *
     * @param symbolicName the symbolic name of the bundle to uninstall
     * @throws BundleException if the bundle cannot be uninstalled
     */
    public void uninstallPlugin(final String symbolicName) throws BundleException {
        Objects.requireNonNull(symbolicName, "symbolicName must not be null");
        final Bundle bundle = installedBundles.remove(symbolicName);
        if (bundle == null) {
            throw new IllegalArgumentException("No bundle found with symbolic name: " + symbolicName);
        }
        bundle.uninstall();
        if (pluginRegistry != null) {
            pluginRegistry.refresh();
        }
        LOG.info("Uninstalled plugin bundle: {}", symbolicName);
    }

    /**
     * Returns the state of a specific bundle.
     *
     * @param symbolicName the bundle symbolic name
     * @return the bundle state, or -1 if not found
     */
    public int getBundleState(final String symbolicName) {
        final Bundle bundle = installedBundles.get(symbolicName);
        return bundle != null ? bundle.getState() : -1;
    }

    /**
     * Returns an unmodifiable view of all installed bundle names and their states.
     *
     * @return map of symbolic name to bundle state
     */
    public Map<String, Integer> getInstalledBundles() {
        final Map<String, Integer> states = new HashMap<>();
        for (final Map.Entry<String, Bundle> entry : installedBundles.entrySet()) {
            states.put(entry.getKey(), entry.getValue().getState());
        }
        return Collections.unmodifiableMap(states);
    }
}
