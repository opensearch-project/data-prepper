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

/**
 * Seam interface for future functional health probes of OSGi plugin bundles.
 * <p>
 * Structural health (bundle RESOLVED/ACTIVE, classloader isolation) is handled
 * by {@link BundleHealthCheck}. This interface is reserved for <em>functional</em>
 * health — detecting the case where a bundle is ACTIVE but internally broken
 * (e.g. a plugin that starts but cannot process events due to a missing resource).
 * <p>
 * Implementation is explicitly out of scope for the initial OSGi release. Plugin
 * authors would implement this interface and register it as an OSGi service;
 * the framework would poll all registered probes during health checks.
 *
 * <pre>{@code
 * // Future usage example:
 * public class MyPluginHealthProbe implements PluginHealthProbe {
 *     public String getBundleSymbolicName() { return "my.plugin"; }
 *     public boolean isHealthy() { return myInternalState.isGood(); }
 *     public String getStatusMessage() { return "OK"; }
 * }
 * }</pre>
 */
public interface PluginHealthProbe {

    /**
     * Returns the symbolic name of the bundle this probe is associated with.
     *
     * @return the OSGi bundle symbolic name
     */
    String getBundleSymbolicName();

    /**
     * Returns whether the plugin is functionally healthy.
     *
     * @return true if the plugin can process work normally
     */
    boolean isHealthy();

    /**
     * Returns a human-readable status message describing the current health state.
     *
     * @return a status message (never null)
     */
    String getStatusMessage();
}
