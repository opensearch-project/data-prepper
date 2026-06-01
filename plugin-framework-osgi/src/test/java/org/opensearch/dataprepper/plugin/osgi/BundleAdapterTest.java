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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.osgi.framework.BundleException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BundleAdapterTest {

    private FelixPluginManager felixPluginManager;
    private BundleAdapter bundleAdapter;

    @TempDir
    File tempDir;

    @BeforeEach
    void setUp() throws BundleException {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        bundleAdapter = new BundleAdapter(felixPluginManager.getBundleContext());
    }

    @Test
    void constructor_with_null_context_throws() {
        assertThrows(NullPointerException.class, () -> new BundleAdapter(null));
    }

    @Test
    void adaptAndInstall_with_null_jar_throws() {
        assertThrows(NullPointerException.class, () -> bundleAdapter.adaptAndInstall(null));
    }

    @Test
    void adaptAndInstall_with_nonexistent_jar_throws() {
        assertThrows(IllegalArgumentException.class,
                () -> bundleAdapter.adaptAndInstall(new File("/nonexistent.jar")));
    }

    @Test
    void isOsgiBundle_returns_false_for_plain_jar() throws IOException {
        final File plainJar = createPlainJar("plain-plugin");
        assertFalse(bundleAdapter.isOsgiBundle(plainJar));
    }

    @Test
    void isOsgiBundle_returns_true_for_osgi_jar() throws IOException {
        final File osgiJar = createOsgiJar("osgi-plugin");
        assertTrue(bundleAdapter.isOsgiBundle(osgiJar));
    }

    @Test
    void createAdaptedBundle_produces_valid_jar() throws IOException {
        final File plainJar = createPlainJar("test-plugin");
        final File adapted = bundleAdapter.createAdaptedBundle(plainJar);

        assertThat(adapted, is(notNullValue()));
        assertTrue(adapted.exists());
        assertTrue(bundleAdapter.isOsgiBundle(adapted));
    }

    @Test
    void createAdaptedBundle_writes_to_framework_storage_dir() throws IOException {
        final File plainJar = createPlainJar("storage-dir-test");
        final File adapted = bundleAdapter.createAdaptedBundle(plainJar);

        // The adapted bundle should be within the framework data area, not java.io.tmpdir
        final String adaptedPath = adapted.getAbsolutePath();
        assertThat("Adapted bundle should be in adapted-bundles subdir",
                adaptedPath.contains("adapted-bundles"), is(true));
        assertFalse(adapted.getName().contains("-") && adapted.getName().matches(".*\\d{5,}.*"),
                "Adapted bundle should not use temp file naming with random digits");
    }

    @Test
    void adaptAndInstall_installs_plain_jar_as_bundle() throws Exception {
        final File plainJar = createPlainJar("installable-plugin");
        final var bundle = bundleAdapter.adaptAndInstall(plainJar);

        assertThat(bundle, is(notNullValue()));
        assertThat(bundle.getSymbolicName(), is(notNullValue()));

        felixPluginManager.close();
    }

    @Test
    void adaptAndInstall_installs_osgi_jar_directly() throws Exception {
        final File osgiJar = createOsgiJar("direct-osgi-plugin");
        final var bundle = bundleAdapter.adaptAndInstall(osgiJar);

        assertThat(bundle, is(notNullValue()));
        assertThat(bundle.getSymbolicName(), is("test.direct-osgi-plugin"));

        felixPluginManager.close();
    }

    private File createPlainJar(final String name) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            // Empty JAR with just manifest
        }
        return jar;
    }

    private File createOsgiJar(final String name) throws IOException {
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
