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
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DynamicLoadTest {

    private FelixPluginManager felixPluginManager;
    private PluginHotLoader hotLoader;

    @TempDir
    File tempDir;

    @BeforeEach
    void setUp() throws BundleException {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        hotLoader = new PluginHotLoader(felixPluginManager.getBundleContext());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (felixPluginManager != null) {
            felixPluginManager.close();
        }
    }

    @Test
    void installPlugin_installs_and_starts_bundle() throws Exception {
        final File jar = createTestOsgiJar("hot-load-test");
        final String symbolicName = hotLoader.installPlugin(jar);

        assertThat(symbolicName, is(notNullValue()));
        assertThat(hotLoader.getBundleState(symbolicName), is(Bundle.ACTIVE));
    }

    @Test
    void uninstallPlugin_removes_bundle() throws Exception {
        final File jar = createTestOsgiJar("uninstall-test");
        final String symbolicName = hotLoader.installPlugin(jar);

        hotLoader.uninstallPlugin(symbolicName);
        assertThat(hotLoader.getBundleState(symbolicName), is(-1));
    }

    @Test
    void uninstallPlugin_with_unknown_name_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> hotLoader.uninstallPlugin("nonexistent.bundle"));
    }

    @Test
    void getInstalledBundles_returns_all_installed() throws Exception {
        final File jar1 = createTestOsgiJar("bundle-a");
        final File jar2 = createTestOsgiJar("bundle-b");

        hotLoader.installPlugin(jar1);
        hotLoader.installPlugin(jar2);

        final Map<String, Integer> bundles = hotLoader.getInstalledBundles();
        assertThat(bundles.size(), is(2));
        assertTrue(bundles.values().stream().allMatch(state -> state == Bundle.ACTIVE));
    }

    @Test
    void installPlugin_with_null_throws() {
        assertThrows(NullPointerException.class, () -> hotLoader.installPlugin(null));
    }

    private File createTestOsgiJar(final String name) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Bundle-SymbolicName", "test." + name);
        manifest.getMainAttributes().putValue("Bundle-Version", "1.0.0");
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            // Empty OSGi bundle
        }
        return jar;
    }
}
