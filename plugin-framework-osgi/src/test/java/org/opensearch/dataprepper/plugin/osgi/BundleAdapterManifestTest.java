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
import org.osgi.framework.BundleException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for BundleAdapter focusing on manifest generation and plugin discovery.
 */
class BundleAdapterManifestTest {

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

    @AfterEach
    void tearDown() throws Exception {
        if (felixPluginManager != null) {
            felixPluginManager.close();
        }
    }

    @Test
    void createAdaptedBundle_with_classes_includes_packages_in_export() throws IOException {
        final File jar = createJarWithClasses("pkg-test");
        final File adapted = bundleAdapter.createAdaptedBundle(jar);

        assertTrue(adapted.exists());
        try (java.util.jar.JarFile adaptedJar = new java.util.jar.JarFile(adapted)) {
            final String exportPkg = adaptedJar.getManifest().getMainAttributes().getValue("Export-Package");
            assertThat(exportPkg, is(notNullValue()));
            assertTrue(exportPkg.contains("com.example.plugin"));
        }
    }

    @Test
    void createAdaptedBundle_with_plugin_properties_includes_header() throws IOException {
        final File jar = createJarWithPluginProperties("props-test");
        final File adapted = bundleAdapter.createAdaptedBundle(jar);

        assertTrue(adapted.exists());
        try (java.util.jar.JarFile adaptedJar = new java.util.jar.JarFile(adapted)) {
            final String pluginClasses = adaptedJar.getManifest().getMainAttributes()
                    .getValue("DataPrepper-Plugin-Classes");
            assertThat(pluginClasses, is(notNullValue()));
            assertTrue(pluginClasses.contains("org.opensearch.dataprepper.plugins"));
        }
    }

    @Test
    void createAdaptedBundle_without_manifest_creates_valid_bundle() throws IOException {
        final File jar = createJarWithoutManifest("no-manifest");
        final File adapted = bundleAdapter.createAdaptedBundle(jar);

        assertTrue(adapted.exists());
        assertTrue(bundleAdapter.isOsgiBundle(adapted));
    }

    @Test
    void createAdaptedBundle_preserves_existing_entries() throws IOException {
        final File jar = createJarWithClasses("preserve-test");
        final File adapted = bundleAdapter.createAdaptedBundle(jar);

        try (java.util.jar.JarFile adaptedJar = new java.util.jar.JarFile(adapted)) {
            assertThat(adaptedJar.getEntry("com/example/plugin/MyPlugin.class"), is(notNullValue()));
        }
    }

    private File createJarWithClasses(final String name) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            jos.putNextEntry(new ZipEntry("com/example/plugin/MyPlugin.class"));
            jos.write(new byte[]{0});
            jos.closeEntry();
        }
        return jar;
    }

    private File createJarWithPluginProperties(final String name) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            jos.putNextEntry(new ZipEntry("META-INF/data-prepper.plugins.properties"));
            final Properties props = new Properties();
            props.setProperty("org.opensearch.dataprepper.plugin.packages",
                    "org.opensearch.dataprepper.plugins");
            props.store(jos, null);
            jos.closeEntry();
        }
        return jar;
    }

    private File createJarWithoutManifest(final String name) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            jos.putNextEntry(new ZipEntry("com/example/Empty.class"));
            jos.write(new byte[]{0});
            jos.closeEntry();
        }
        return jar;
    }
}
