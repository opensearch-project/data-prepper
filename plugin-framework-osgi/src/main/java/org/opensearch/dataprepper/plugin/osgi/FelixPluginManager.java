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

import org.apache.felix.framework.FrameworkFactory;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.osgi.framework.FrameworkEvent;
import org.osgi.framework.launch.Framework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Manages the embedded Apache Felix OSGi framework lifecycle.
 * Handles init, start, stop, and destroy of the Felix container.
 */
public class FelixPluginManager implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(FelixPluginManager.class);

    static final String DATA_PREPPER_DIR_PROPERTY = "data-prepper.dir";

    private final Framework framework;

    public FelixPluginManager() {
        this(createDefaultConfig());
    }

    public FelixPluginManager(final Map<String, String> felixConfig) {
        Objects.requireNonNull(felixConfig, "felixConfig must not be null");
        this.framework = new FrameworkFactory().newFramework(felixConfig);
    }

    /**
     * Initializes and starts the Felix framework.
     *
     * @throws BundleException if the framework fails to start
     */
    public void start() throws BundleException {
        LOG.info("Starting embedded Felix OSGi framework");
        framework.start();
        LOG.info("Felix OSGi framework started. State: {}", StaticBundleLoader.getStateString(framework.getState()));
    }

    /**
     * Stops the Felix framework and waits for it to fully stop.
     *
     * @throws BundleException if the framework fails to stop
     * @throws InterruptedException if interrupted while waiting for shutdown
     */
    public void stop() throws BundleException, InterruptedException {
        LOG.info("Stopping embedded Felix OSGi framework");
        framework.stop();
        final FrameworkEvent stopEvent = framework.waitForStop(30_000);
        if (stopEvent.getType() == FrameworkEvent.WAIT_TIMEDOUT) {
            LOG.warn("Felix OSGi framework did not stop within 30 seconds (WAIT_TIMEDOUT)");
        } else if (stopEvent.getType() == FrameworkEvent.ERROR) {
            LOG.error("Felix OSGi framework stop encountered an error", stopEvent.getThrowable());
        } else {
            LOG.info("Felix OSGi framework stopped");
        }
    }

    /**
     * Returns the BundleContext for the system bundle.
     *
     * @return the system BundleContext, or null if framework is not started
     */
    public BundleContext getBundleContext() {
        return framework.getBundleContext();
    }

    /**
     * Installs a bundle from the given location.
     *
     * @param location the bundle location (file URI or URL)
     * @return the installed Bundle
     * @throws BundleException if installation fails
     */
    public Bundle installBundle(final String location) throws BundleException {
        Objects.requireNonNull(location, "Bundle location must not be null");
        final BundleContext context = getBundleContext();
        if (context == null) {
            throw new IllegalStateException("Framework is not started");
        }
        final Bundle bundle = context.installBundle(location);
        LOG.debug("Installed bundle: {} [{}]", bundle.getSymbolicName(), bundle.getBundleId());
        return bundle;
    }

    /**
     * Returns the current state of the framework.
     *
     * @return the framework state as an integer constant from {@link Bundle}
     */
    public int getFrameworkState() {
        return framework.getState();
    }

    /**
     * Returns whether the framework is currently active.
     *
     * @return true if the framework is in ACTIVE state
     */
    public boolean isActive() {
        return framework.getState() == Bundle.ACTIVE;
    }

    @Override
    public void close() throws Exception {
        if (framework.getState() == Bundle.ACTIVE) {
            try {
                stop();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            } catch (final BundleException e) {
                LOG.warn("Error stopping OSGi framework during close", e);
            }
        }
    }

    Framework getFramework() {
        return framework;
    }

    private static Map<String, String> createDefaultConfig() {
        final Map<String, String> config = new HashMap<>();
        final long pid = ProcessHandle.current().pid();
        final String baseDir = resolveBaseDirectory();
        final String cacheDir = baseDir + File.separator + "data" + File.separator
                + "osgi" + File.separator + "felix-cache-" + pid;
        LOG.info("Felix OSGi cache directory: {}", cacheDir);
        config.put(Constants.FRAMEWORK_STORAGE, cacheDir);
        config.put(Constants.FRAMEWORK_STORAGE_CLEAN, Constants.FRAMEWORK_STORAGE_CLEAN_ONFIRSTINIT);
        config.put(Constants.FRAMEWORK_SYSTEMPACKAGES_EXTRA,
                DataPrepperOsgiPackages.buildSystemPackagesExtra());
        // Provide the osgi.ee capability so bundles with Require-Capability for JavaSE resolve.
        // Use "extra" to append to defaults rather than replace them.
        config.put("org.osgi.framework.system.capabilities.extra",
                buildExecutionEnvironmentCapability());
        return config;
    }

    /**
     * Builds the osgi.ee system capability string for the current JVM.
     * Advertises JavaSE versions from 1.0 up to the current runtime version.
     */
    private static String buildExecutionEnvironmentCapability() {
        final int javaVersion = Runtime.version().feature();
        final StringBuilder versions = new StringBuilder();
        // Advertise 1.x versions (1.0 through 1.8)
        for (int v = 0; v <= 8; v++) {
            if (versions.length() > 0) {
                versions.append(", ");
            }
            versions.append("1.").append(v);
        }
        // Advertise 9+ versions up to current
        for (int v = 9; v <= javaVersion; v++) {
            versions.append(", ").append(v).append(".0");
        }
        return "osgi.ee; osgi.ee=\"JavaSE\"; version:List<Version>=\"" + versions + "\"";
    }

    /**
     * Resolves the base directory for OSGi storage. Uses the {@code data-prepper.dir}
     * system property if set (production), otherwise falls back to {@code java.io.tmpdir}
     * (needed for unit tests that don't set the property).
     */
    private static String resolveBaseDirectory() {
        final String dataPrepperDir = System.getProperty(DATA_PREPPER_DIR_PROPERTY);
        if (dataPrepperDir != null && !dataPrepperDir.isEmpty()) {
            LOG.debug("Using data-prepper.dir for OSGi storage: {}", dataPrepperDir);
            return dataPrepperDir;
        }
        final String tmpDir = System.getProperty("java.io.tmpdir");
        LOG.debug("data-prepper.dir not set; falling back to java.io.tmpdir for OSGi storage: {}", tmpDir);
        return tmpDir;
    }
}
