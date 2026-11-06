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

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import org.opensearch.dataprepper.plugin.PluginProviderLoader;
import org.opensearch.dataprepper.plugin.PluginProviderRegistrar;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;
import java.io.File;
import java.util.Optional;

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
 * {@code data-prepper.plugin.bundles.dir}. If not set, the framework starts with no plugin bundles
 * (host packages only), which is valid for testing.
 * <p>
 * Activated only when {@code -Ddata-prepper.plugin.framework=osgi} is set.
 * <p>
 * This bean is the {@link PluginProviderRegistrar} for the OSGi framework.
 * {@link org.opensearch.dataprepper.plugin.DefaultPluginFactory} depends on that interface, so Spring
 * must run {@link #initialize()} to completion before the plugin factory — and therefore any pipeline
 * construction — exists.
 */
@Named
public class OsgiFrameworkRunner implements PluginProviderRegistrar {
    private static final Logger LOG = LoggerFactory.getLogger(OsgiFrameworkRunner.class);
    static final String PLUGIN_FRAMEWORK_PROPERTY = "data-prepper.plugin.framework";
    static final String BUNDLES_DIR_PROPERTY = "data-prepper.plugin.bundles.dir";
    private static final String MODE_OSGI = "osgi";

    private final PluginProviderLoader pluginProviderLoader;
    private final MeterRegistry meterRegistry;
    private final boolean osgiEnabled;
    private FelixPluginManager felixPluginManager;
    private OsgiPluginRegistry osgiPluginRegistry;
    private BundleHealthCheck bundleHealthCheck;
    private volatile boolean pluginProviderRegistrationComplete;

    /**
     * @param pluginProviderLoader the loader the OSGi plugin registry is registered into
     * @param meterRegistry        Data Prepper's composite meter registry. This is injected rather
     *                             than taken from {@code Metrics.globalRegistry} so that OSGi
     *                             metrics land in the registries Data Prepper actually exports, and
     *                             so that Spring orders registry creation before framework startup.
     *                             It is optional because this bean is discovered by package scan:
     *                             several contexts scan the OSGi package without the metrics
     *                             configuration, and a required dependency would break them. When
     *                             absent, metrics are recorded into a detached registry — never into
     *                             {@code Metrics.globalRegistry}, which Data Prepper does not export.
     */
    @Inject
    public OsgiFrameworkRunner(final PluginProviderLoader pluginProviderLoader,
                               final Optional<CompositeMeterRegistry> meterRegistry) {
        this.pluginProviderLoader = pluginProviderLoader;
        this.meterRegistry = meterRegistry.orElseGet(() -> {
            LOG.debug("No CompositeMeterRegistry bean is available; OSGi metrics will not be exported.");
            return new CompositeMeterRegistry();
        });
        this.osgiEnabled = isOsgiMode();
    }

    @PostConstruct
    void initialize() {
        if (!osgiEnabled) {
            LOG.debug("OSGi plugin framework is not enabled. Set -D{}={} to enable.",
                    PLUGIN_FRAMEWORK_PROPERTY, MODE_OSGI);
            pluginProviderRegistrationComplete = true;
            return;
        }

        try {
            startFramework();
            loadBundles();
            registerPluginProvider();
            bundleHealthCheck = new BundleHealthCheck(felixPluginManager.getBundleContext());
            pluginProviderRegistrationComplete = true;
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
        final StaticBundleLoader loader = new StaticBundleLoader(ctx, meterRegistry);
        loader.loadBundles(dir);
    }

    private void registerPluginProvider() {
        osgiPluginRegistry = new OsgiPluginRegistry(felixPluginManager.getBundleContext());
        pluginProviderLoader.registerProvider(osgiPluginRegistry);
    }

    /**
     * {@inheritDoc}
     * <p>
     * Registration is reported complete once {@link #initialize()} has finished, including when OSGi
     * mode is disabled and there is consequently nothing to register.
     */
    @Override
    public boolean isPluginProviderRegistrationComplete() {
        return pluginProviderRegistrationComplete;
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
