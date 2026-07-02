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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;

class LegacyPluginBundleActivatorTest {

    private FelixPluginManager felixPluginManager;
    private LegacyPluginBundleActivator activator;

    @BeforeEach
    void setUp() throws BundleException {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        activator = new LegacyPluginBundleActivator();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (felixPluginManager != null) {
            felixPluginManager.close();
        }
    }

    @Test
    void start_with_no_plugin_classes_header_does_nothing() throws Exception {
        final BundleContext ctx = felixPluginManager.getBundleContext();
        // System bundle has no DataPrepper-Plugin-Classes header
        activator.start(ctx);
        // Should complete without error
    }

    @Test
    void stop_with_no_registrations_does_nothing() {
        final BundleContext ctx = felixPluginManager.getBundleContext();
        activator.stop(ctx);
        // Should complete without error
    }

    @Test
    void start_then_stop_lifecycle() throws Exception {
        final BundleContext ctx = felixPluginManager.getBundleContext();
        activator.start(ctx);
        activator.stop(ctx);
        // Should complete without error
    }
}
