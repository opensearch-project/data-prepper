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
import org.osgi.framework.wiring.BundleWiring;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A try-with-resources helper that sets the Thread Context ClassLoader (TCCL) to a
 * bundle's classloader on construction and restores the previous TCCL on {@link #close()}.
 * <p>
 * Under OSGi, {@link java.util.ServiceLoader#load(Class)} uses the TCCL to discover service
 * implementations. The default TCCL in an OSGi environment is NOT the bundle's classloader,
 * so SPI lookups fail at runtime. This scope ensures that plugin code executed within it
 * can rely on the standard {@code ServiceLoader.load(X)} idiom to discover implementations
 * packaged in the bundle's {@code META-INF/services} directory.
 * <p>
 * Usage:
 * <pre>{@code
 * try (BundleClassLoaderScope scope = BundleClassLoaderScope.of(bundle)) {
 *     // plugin code here — ServiceLoader.load(X) works against the bundle classloader
 * }
 * // TCCL is restored even if an exception was thrown
 * }</pre>
 */
public final class BundleClassLoaderScope implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(BundleClassLoaderScope.class);

    private final ClassLoader previousClassLoader;
    private final Thread thread;

    private BundleClassLoaderScope(final ClassLoader bundleClassLoader) {
        this.thread = Thread.currentThread();
        this.previousClassLoader = thread.getContextClassLoader();
        if (bundleClassLoader != null) {
            thread.setContextClassLoader(bundleClassLoader);
            LOG.trace("TCCL set to bundle classloader: {}", bundleClassLoader);
        } else {
            LOG.trace("Bundle classloader is null; TCCL unchanged (will still restore on close)");
        }
    }

    /**
     * Creates a scope that sets the TCCL to the classloader of the given bundle.
     * If the bundle has no wiring or no classloader (e.g. an unresolved or fragment bundle),
     * the TCCL is left unchanged but will still be properly restored on {@link #close()}.
     *
     * @param bundle the OSGi bundle whose classloader should become the TCCL
     * @return a new scope; must be closed to restore the previous TCCL
     */
    public static BundleClassLoaderScope of(final Bundle bundle) {
        ClassLoader bundleClassLoader = null;
        if (bundle != null) {
            final BundleWiring wiring = bundle.adapt(BundleWiring.class);
            if (wiring != null) {
                bundleClassLoader = wiring.getClassLoader();
            }
        }
        return new BundleClassLoaderScope(bundleClassLoader);
    }

    /**
     * Creates a scope with an explicit classloader. Useful for testing or when the
     * classloader is already known.
     *
     * @param classLoader the classloader to set as the TCCL, may be null
     * @return a new scope; must be closed to restore the previous TCCL
     */
    public static BundleClassLoaderScope of(final ClassLoader classLoader) {
        return new BundleClassLoaderScope(classLoader);
    }

    /**
     * Restores the TCCL to its previous value. Safe to call multiple times.
     * Must be closed on the same thread that opened it.
     */
    @Override
    public void close() {
        thread.setContextClassLoader(previousClassLoader);
        LOG.trace("TCCL restored to: {}", previousClassLoader);
    }
}
