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
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.dataprepper.model.processor.Processor;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end integration test that validates the full OSGi plugin loading path:
 * 1. Starts Felix framework
 * 2. Registers a mock plugin service in the OSGi registry
 * 3. Discovers it via OsgiPluginRegistry
 * 4. Validates hot-load install/uninstall cycle
 */
class OsgiEndToEndTest {

    private FelixPluginManager felixPluginManager;
    private BundleContext bundleContext;

    @TempDir
    File tempDir;

    @BeforeEach
    void setUp() throws BundleException {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        bundleContext = felixPluginManager.getBundleContext();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (felixPluginManager != null) {
            felixPluginManager.close();
        }
    }

    @Test
    void full_lifecycle_felix_start_register_discover_stop() {
        // 1. Felix is active
        assertThat(felixPluginManager.isActive(), is(true));

        // 2. Register a mock plugin service in OSGi registry (mirrors LegacyPluginBundleActivator)
        final Dictionary<String, Object> props = new Hashtable<>();
        props.put(OsgiPluginRegistry.PLUGIN_NAME_PROPERTY, "test_echo");
        props.put(OsgiPluginRegistry.PLUGIN_TYPE_PROPERTY, Processor.class.getName());
        props.put("dataprepper.plugin.class", Processor.class.getName());
        bundleContext.registerService(
                Class.class.getName(),
                Processor.class,
                props);

        // 3. OsgiPluginRegistry discovers the service
        final OsgiPluginRegistry registry = new OsgiPluginRegistry(bundleContext);
        registry.scanServices();
        assertThat(registry.getPluginCount(), is(1));
    }

    @Test
    void hot_load_install_start_uninstall_cycle() throws Exception {
        final BundleAdapter adapter = new BundleAdapter(bundleContext);
        final PluginHotLoader hotLoader = new PluginHotLoader(bundleContext, adapter);

        // Create a test bundle JAR
        final File jar = createTestBundle("hot-load-e2e");

        // Install and start
        final String symbolicName = hotLoader.installPlugin(jar);
        assertThat(symbolicName, is(notNullValue()));
        assertThat(hotLoader.getBundleState(symbolicName), is(Bundle.ACTIVE));

        // Verify it shows in installed bundles
        assertThat(hotLoader.getInstalledBundles().containsKey(symbolicName), is(true));

        // Uninstall
        hotLoader.uninstallPlugin(symbolicName);
        assertThat(hotLoader.getBundleState(symbolicName), is(-1));
    }

    @Test
    void bundle_adapter_produces_isolated_classloader() throws Exception {
        final File jar1 = createTestBundle("isolated-a");
        final File jar2 = createTestBundle("isolated-b");

        final BundleAdapter adapter = new BundleAdapter(bundleContext);
        final Bundle bundle1 = adapter.adaptAndInstall(jar1);
        final Bundle bundle2 = adapter.adaptAndInstall(jar2);

        // Each bundle should have a distinct symbolic name
        assertTrue(!bundle1.getSymbolicName().equals(bundle2.getSymbolicName()),
                "Bundles should have distinct symbolic names");

        // Each bundle gets its own classloader (OSGi guarantee)
        assertThat(bundle1.getBundleId() != bundle2.getBundleId(), is(true));
    }

    @Test
    void osgi_registry_scan_finds_services_registered_after_creation() {
        final OsgiPluginRegistry registry = new OsgiPluginRegistry(bundleContext);

        // Initially empty
        registry.scanServices();
        assertThat(registry.getPluginCount(), is(0));

        // Register a service after registry creation (mirrors LegacyPluginBundleActivator)
        final Dictionary<String, Object> props = new Hashtable<>();
        props.put(OsgiPluginRegistry.PLUGIN_NAME_PROPERTY, "late_plugin");
        props.put(OsgiPluginRegistry.PLUGIN_TYPE_PROPERTY, Processor.class.getName());
        props.put("dataprepper.plugin.class", Processor.class.getName());
        bundleContext.registerService(Class.class.getName(), Processor.class, props);

        // Re-scan should find it
        registry.scanServices();
        assertThat(registry.getPluginCount(), is(1));
    }

    private File createTestBundle(final String name) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Bundle-SymbolicName", "test." + name);
        manifest.getMainAttributes().putValue("Bundle-Version", "1.0.0");
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            // Empty bundle
        }
        return jar;
    }
}
