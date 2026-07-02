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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.wiring.FrameworkWiring;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Performs a static bundle install/resolve/start sequence at startup. This replaces
 * any hot-load directory scan: bundles are installed once, forced to resolve via
 * {@link FrameworkWiring#resolveBundles(Collection)}, verified to be in RESOLVED
 * or ACTIVE state, and then started. Any failure is translated to a human-readable
 * diagnostic and thrown as a {@link BundleLoadException} for fail-fast startup.
 * <p>
 * After the sequence, a startup summary log is emitted showing each bundle's final
 * state and totals.
 */
final class StaticBundleLoader {
    private static final Logger LOG = LoggerFactory.getLogger(StaticBundleLoader.class);

    static final String METRIC_BUNDLES_LOADED = "osgi.plugin.bundlesLoaded";
    static final String METRIC_BUNDLES_FAILED = "osgi.plugin.bundlesFailed";
    static final String METRIC_RESOLUTION_DURATION = "osgi.plugin.resolutionDuration";
    static final String METRIC_BUNDLES_ACTIVE = "osgi.plugin.bundlesActive";

    private final BundleContext bundleContext;
    private final Counter bundlesLoadedCounter;
    private final Counter bundlesFailedCounter;
    private final Timer resolutionTimer;
    private final AtomicInteger activeBundleCount;

    StaticBundleLoader(final BundleContext bundleContext) {
        this(bundleContext, Metrics.globalRegistry);
    }

    StaticBundleLoader(final BundleContext bundleContext, final MeterRegistry meterRegistry) {
        this.bundleContext = Objects.requireNonNull(bundleContext, "bundleContext must not be null");
        Objects.requireNonNull(meterRegistry, "meterRegistry must not be null");
        this.bundlesLoadedCounter = meterRegistry.counter(METRIC_BUNDLES_LOADED);
        this.bundlesFailedCounter = meterRegistry.counter(METRIC_BUNDLES_FAILED);
        this.resolutionTimer = meterRegistry.timer(METRIC_RESOLUTION_DURATION);
        this.activeBundleCount = new AtomicInteger(0);
        Gauge.builder(METRIC_BUNDLES_ACTIVE, activeBundleCount, AtomicInteger::get)
                .register(meterRegistry);
    }

    /**
     * Installs, resolves, and starts all bundle JARs found in the given directory.
     * On any resolution or activation failure, throws with a translated diagnostic message.
     *
     * @param bundlesDir the directory containing bundle JAR files
     * @return the list of successfully started bundles
     * @throws BundleLoadException if any bundle fails to resolve or start
     */
    List<Bundle> loadBundles(final File bundlesDir) {
        Objects.requireNonNull(bundlesDir, "bundlesDir must not be null");
        if (!bundlesDir.isDirectory()) {
            throw new BundleLoadException("Bundle directory does not exist or is not a directory: " + bundlesDir);
        }

        final File[] jarFiles = bundlesDir.listFiles((dir, name) -> name.endsWith(".jar"));
        if (jarFiles == null || jarFiles.length == 0) {
            LOG.info("No bundle JARs found in {}", bundlesDir);
            logStartupSummary(Collections.emptyList());
            return Collections.emptyList();
        }

        return loadBundles(Arrays.asList(jarFiles));
    }

    /**
     * Installs, resolves, and starts the specified bundle JAR files.
     *
     * @param jarFiles the bundle JAR files to load
     * @return the list of successfully started bundles
     * @throws BundleLoadException if any bundle fails to resolve or start
     */
    List<Bundle> loadBundles(final List<File> jarFiles) {
        Objects.requireNonNull(jarFiles, "jarFiles must not be null");
        if (jarFiles.isEmpty()) {
            logStartupSummary(Collections.emptyList());
            return Collections.emptyList();
        }

        // Phase 1: Install all bundles
        final List<Bundle> installed = installBundles(jarFiles);

        // Phase 2: Force resolution
        final Timer.Sample resolutionSample = Timer.start();
        try {
            resolveBundles(installed);
        } finally {
            resolutionSample.stop(resolutionTimer);
        }

        // Phase 3: Verify resolution — detect failures
        final List<Bundle> unresolved = installed.stream()
                .filter(b -> b.getState() < Bundle.RESOLVED)
                .collect(Collectors.toList());

        if (!unresolved.isEmpty()) {
            bundlesFailedCounter.increment(unresolved.size());
            logStartupSummary(installed);
            final String errorMessages = unresolved.stream()
                    .map(b -> BundleResolutionErrorTranslator.translateMessage(
                            b.getSymbolicName(),
                            "Bundle state is " + getStateString(b.getState()) + " (not RESOLVED)"))
                    .collect(Collectors.joining("; "));
            throw new BundleLoadException("Bundle resolution failed: " + errorMessages);
        }

        // Phase 4: Start all resolved bundles
        final List<Bundle> started = startBundles(installed);

        // Update metrics
        bundlesLoadedCounter.increment(started.size());
        activeBundleCount.set(countActiveBundles());

        logStartupSummary(installed);
        return started;
    }

    private List<Bundle> installBundles(final List<File> jarFiles) {
        final List<Bundle> bundles = new ArrayList<>(jarFiles.size());
        for (final File jarFile : jarFiles) {
            try {
                final String location = jarFile.toURI().toString();
                final Bundle bundle = bundleContext.installBundle(location);
                validateOsgiBundle(bundle, jarFile);
                bundles.add(bundle);
                LOG.info("Installed OSGi bundle directly: {} [id={}]",
                        bundle.getSymbolicName(), bundle.getBundleId());
            } catch (final BundleLoadException e) {
                bundlesFailedCounter.increment();
                throw e;
            } catch (final BundleException e) {
                bundlesFailedCounter.increment();
                throw new BundleLoadException(
                        BundleResolutionErrorTranslator.translate(jarFile.getName(), e), e);
            }
        }
        return bundles;
    }

    /**
     * Validates that an installed bundle is a proper OSGi bundle (has a Bundle-SymbolicName).
     * Plugin JARs must arrive already-OSGi with manifests baked by the Gradle plugin.
     * A JAR lacking a Bundle-SymbolicName is a fail-fast error.
     */
    private void validateOsgiBundle(final Bundle bundle, final File jarFile) {
        if (bundle.getSymbolicName() == null) {
            LOG.error("Plugin JAR '{}' is not an OSGi bundle (missing Bundle-SymbolicName). "
                            + "Apply the 'org.opensearch.dataprepper.osgi' Gradle plugin to the plugin's "
                            + "build to bake an OSGi manifest at build time.",
                    jarFile.getName());
            try {
                bundle.uninstall();
            } catch (final BundleException e) {
                LOG.warn("Failed to uninstall invalid bundle from JAR: {}", jarFile.getName(), e);
            }
            throw new BundleLoadException(
                    "Plugin JAR '" + jarFile.getName() + "' is not an OSGi bundle "
                            + "(missing Bundle-SymbolicName). Apply the "
                            + "'org.opensearch.dataprepper.osgi' Gradle plugin to the plugin's "
                            + "build to bake an OSGi manifest at build time.");
        }
    }

    private void resolveBundles(final List<Bundle> bundles) {
        final FrameworkWiring frameworkWiring = bundleContext.getBundle(0)
                .adapt(FrameworkWiring.class);
        if (frameworkWiring == null) {
            throw new BundleLoadException("Cannot obtain FrameworkWiring from system bundle");
        }
        frameworkWiring.resolveBundles(bundles);
    }

    private List<Bundle> startBundles(final List<Bundle> bundles) {
        final List<Bundle> started = new ArrayList<>(bundles.size());
        for (final Bundle bundle : bundles) {
            // Skip fragment bundles — they cannot be started
            if (bundle.getHeaders().get("Fragment-Host") != null) {
                LOG.debug("Skipping fragment bundle: {}", bundle.getSymbolicName());
                started.add(bundle);
                continue;
            }
            try {
                bundle.start();
                started.add(bundle);
            } catch (final BundleException e) {
                bundlesFailedCounter.increment();
                throw new BundleLoadException(
                        BundleResolutionErrorTranslator.translate(bundle.getSymbolicName(), e), e);
            }
        }
        return started;
    }

    /**
     * Logs the startup summary: one line per bundle and a totals line.
     */
    void logStartupSummary(final List<Bundle> bundles) {
        if (bundles.isEmpty()) {
            LOG.info("OSGi startup: 0 bundles loaded");
            return;
        }

        int activeCount = 0;
        int failedCount = 0;

        for (final Bundle bundle : bundles) {
            final String state = getStateString(bundle.getState());
            LOG.info("  Bundle: {} — {}", bundle.getSymbolicName(), state);
            if (bundle.getState() == Bundle.ACTIVE || bundle.getState() == Bundle.RESOLVED) {
                activeCount++;
            } else {
                failedCount++;
            }
        }

        LOG.info("OSGi startup: {} bundles, {} ACTIVE/RESOLVED, {} failed",
                bundles.size(), activeCount, failedCount);
    }

    private int countActiveBundles() {
        int count = 0;
        // Count non-system bundles that are ACTIVE
        for (final Bundle bundle : bundleContext.getBundles()) {
            if (bundle.getBundleId() != 0 && bundle.getState() == Bundle.ACTIVE) {
                count++;
            }
        }
        return count;
    }

    static String getStateString(final int state) {
        switch (state) {
            case Bundle.UNINSTALLED: return "UNINSTALLED";
            case Bundle.INSTALLED: return "INSTALLED";
            case Bundle.RESOLVED: return "RESOLVED";
            case Bundle.STARTING: return "STARTING";
            case Bundle.STOPPING: return "STOPPING";
            case Bundle.ACTIVE: return "ACTIVE";
            default: return "UNKNOWN(" + state + ")";
        }
    }
}
