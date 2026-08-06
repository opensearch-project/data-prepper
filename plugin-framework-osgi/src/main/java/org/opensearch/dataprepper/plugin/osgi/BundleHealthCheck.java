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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Reports structural health of the OSGi framework and its bundles.
 * <p>
 * Structural health means:
 * <ul>
 *   <li>The framework system bundle is ACTIVE</li>
 *   <li>Each non-system bundle is in RESOLVED or ACTIVE state</li>
 *   <li>Classloader isolation is verified (each bundle has its own ClassLoader
 *       distinct from the system bundle's ClassLoader)</li>
 * </ul>
 * <p>
 * Functional health (ACTIVE-but-internally-broken) is explicitly out of scope.
 * See {@link PluginHealthProbe} for the planned seam for functional probes.
 *
 * <p><strong>Integration note:</strong> Data Prepper's server module
 * ({@code DataPrepperServer}) exposes an HTTP server with metrics/prometheus
 * and pipeline listing but does not have a generic pluggable health-check
 * mechanism as of this writing. Rather than introducing a large core change,
 * this class exposes a queryable API that can be integrated via an HTTP handler
 * or gRPC endpoint in a follow-up PR.</p>
 *
 * TODO: Wire BundleHealthCheck into Data Prepper's server health endpoint
 *       once a generic health-check framework is introduced (or add a dedicated
 *       /osgi/health HTTP context to DataPrepperServer).
 */
public class BundleHealthCheck {
    private static final Logger LOG = LoggerFactory.getLogger(BundleHealthCheck.class);

    private final BundleContext bundleContext;

    public BundleHealthCheck(final BundleContext bundleContext) {
        this.bundleContext = Objects.requireNonNull(bundleContext, "bundleContext must not be null");
    }

    /**
     * Returns a map of bundle symbolic name to its current state string
     * for all non-system bundles.
     *
     * @return unmodifiable map of symbolic-name to state
     */
    public Map<String, String> getBundleStatuses() {
        final Map<String, String> statuses = new LinkedHashMap<>();
        for (final Bundle bundle : bundleContext.getBundles()) {
            if (bundle.getBundleId() == 0) {
                continue; // skip system bundle
            }
            statuses.put(bundle.getSymbolicName(), StaticBundleLoader.getStateString(bundle.getState()));
        }
        return Collections.unmodifiableMap(statuses);
    }

    /**
     * Returns whether all structural health checks pass.
     *
     * @return true if framework is ACTIVE, all bundles are RESOLVED/ACTIVE,
     *         and classloader isolation is confirmed
     */
    public boolean isHealthy() {
        if (!isFrameworkActive()) {
            return false;
        }
        if (!getUnhealthyBundles().isEmpty()) {
            return false;
        }
        if (!isClassloaderIsolationPresent()) {
            return false;
        }
        return true;
    }

    /**
     * Returns the list of bundle symbolic names that are NOT in a healthy state
     * (RESOLVED or ACTIVE).
     *
     * @return list of unhealthy bundle symbolic names (may be empty)
     */
    public List<String> getUnhealthyBundles() {
        final List<String> unhealthy = new ArrayList<>();
        for (final Bundle bundle : bundleContext.getBundles()) {
            if (bundle.getBundleId() == 0) {
                continue; // skip system bundle
            }
            if (bundle.getState() != Bundle.RESOLVED && bundle.getState() != Bundle.ACTIVE) {
                unhealthy.add(bundle.getSymbolicName());
            }
        }
        return Collections.unmodifiableList(unhealthy);
    }

    /**
     * Returns whether the OSGi framework system bundle is in ACTIVE state.
     *
     * @return true if the system bundle is ACTIVE
     */
    public boolean isFrameworkActive() {
        final Bundle systemBundle = bundleContext.getBundle(0);
        return systemBundle != null && systemBundle.getState() == Bundle.ACTIVE;
    }

    /**
     * Verifies that classloader isolation is present: each non-system bundle
     * must have a ClassLoader that is distinct from the system bundle's ClassLoader.
     * <p>
     * Note: Bundles in INSTALLED state (not yet resolved) may not have a classloader
     * yet; these are skipped.
     *
     * @return true if all resolved/active bundles have isolated classloaders
     */
    public boolean isClassloaderIsolationPresent() {
        final Bundle systemBundle = bundleContext.getBundle(0);
        if (systemBundle == null) {
            return false;
        }

        final ClassLoader systemClassLoader = systemBundle.getClass().getClassLoader();

        for (final Bundle bundle : bundleContext.getBundles()) {
            if (bundle.getBundleId() == 0) {
                continue;
            }
            // Only check bundles that have been resolved (have a classloader)
            if (bundle.getState() < Bundle.RESOLVED) {
                continue;
            }
            try {
                final org.osgi.framework.wiring.BundleWiring wiring =
                        bundle.adapt(org.osgi.framework.wiring.BundleWiring.class);
                if (wiring == null) {
                    LOG.warn("Bundle {} has no wiring — classloader isolation cannot be verified",
                            bundle.getSymbolicName());
                    return false;
                }
                final ClassLoader wireCl = wiring.getClassLoader();
                if (wireCl == null) {
                    LOG.warn("Bundle {} wiring has null classloader", bundle.getSymbolicName());
                    return false;
                }
                if (wireCl == systemClassLoader) {
                    LOG.warn("Bundle {} shares classloader with system bundle", bundle.getSymbolicName());
                    return false;
                }
            } catch (final Exception e) {
                LOG.debug("Could not verify classloader for bundle {}: {}",
                        bundle.getSymbolicName(), e.getMessage());
            }
        }
        return true;
    }
}
