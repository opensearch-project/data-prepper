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

import io.micrometer.core.instrument.Metrics;
import org.opensearch.dataprepper.plugin.PluginProviderLoader;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;

/**
 * Spring-managed component that integrates the Felix OSGi framework into
 * the Data Prepper lifecycle. Starts Felix on context init, registers the
 * {@link OsgiPluginRegistry} as a {@link org.opensearch.dataprepper.plugin.PluginProvider},
 * and shuts down Felix on context destroy.
 * <p>
 * The framework follows a static lifecycle: bundles are installed at startup
 * and the framework is torn down at shutdown. Any failure during initialization
 * aborts startup (fail-fast).
 * <p>
 * Bundles to install are sourced from the directory specified by the system property
 * {@code plugin.bundles.dir}. If not set, the framework starts with no plugin bundles
 * (host packages only), which is valid for testing.
 * <p>
 * Activated only when {@code -Dplugin.framework=osgi} is set.
 */
@Named
public class OsgiFrameworkRunner {
    private static final Logger LOG = LoggerFactory.getLogger(OsgiFrameworkRunner.class);
    static final String PLUGIN_FRAMEWORK_PROPERTY = "plugin.framework";
    static final String BUNDLES_DIR_PROPERTY = "plugin.bundles.dir";
    private static final String MODE_OSGI = "osgi";

    private final PluginProviderLoader pluginProviderLoader;
    private final boolean osgiEnabled;
    private FelixPluginManager felixPluginManager;
    private OsgiPluginRegistry osgiPluginRegistry;
    private BundleHealthCheck bundleHealthCheck;

    @Inject
    public OsgiFrameworkRunner(final PluginProviderLoader pluginProviderLoader) {
        this.pluginProviderLoader = pluginProviderLoader;
        this.osgiEnabled = isOsgiMode();
    }

    @PostConstruct
    void initialize() {
        if (!osgiEnabled) {
            LOG.debug("OSGi plugin framework is not enabled. Set -D{}={} to enable.",
                    PLUGIN_FRAMEWORK_PROPERTY, MODE_OSGI);
            return;
        }

        try {
            startFramework();
            loadBundles();
            registerPluginProvider();
            bundleHealthCheck = new BundleHealthCheck(felixPluginManager.getBundleContext());
            LOG.info("OSGi plugin framework initialized successfully");
        } catch (final BundleException e) {
            throw new RuntimeException("Failed to start OSGi framework. Aborting startup.", e);
        } catch (final BundleLoadException e) {
            throw new RuntimeException("OSGi bundle loading failed. Aborting startup.", e);
        }
    }

    @PreDestroy
    void shutdown() {
        if (felixPluginManager != null && felixPluginManager.isActive()) {
            try {
                felixPluginManager.stop();
                LOG.info("OSGi framework stopped");
            } catch (final Exception e) {
                LOG.warn("Error stopping OSGi framework", e);
            }
        }
    }

    private void startFramework() throws BundleException {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
    }

    private void loadBundles() {
        final String bundlesDir = System.getProperty(BUNDLES_DIR_PROPERTY);
        if (bundlesDir == null || bundlesDir.isEmpty()) {
            LOG.info("No {} configured; starting OSGi framework with host packages only",
                    BUNDLES_DIR_PROPERTY);
            return;
        }

        final File dir = new File(bundlesDir);
        final BundleContext ctx = felixPluginManager.getBundleContext();
        final BundleAdapter adapter = new BundleAdapter(ctx);
        final StaticBundleLoader loader = new StaticBundleLoader(ctx, adapter, Metrics.globalRegistry);
        loader.loadBundles(dir);
    }

    private void registerPluginProvider() {
        osgiPluginRegistry = new OsgiPluginRegistry(felixPluginManager.getBundleContext());
        pluginProviderLoader.registerProvider(osgiPluginRegistry);
    }

    /**
     * Returns whether OSGi mode is active and the framework is running.
     */
    public boolean isActive() {
        return felixPluginManager != null && felixPluginManager.isActive();
    }

    /**
     * Returns the bundle health check instance, or null if framework is not active.
     */
    public BundleHealthCheck getBundleHealthCheck() {
        return bundleHealthCheck;
    }

    static boolean isOsgiMode() {
        return MODE_OSGI.equalsIgnoreCase(System.getProperty(PLUGIN_FRAMEWORK_PROPERTY));
    }
}
