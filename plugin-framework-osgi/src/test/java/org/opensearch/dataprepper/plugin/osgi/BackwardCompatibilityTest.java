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

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.opensearch.dataprepper.plugin.osgi.fixture.TestLegacyProcessor;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Verifies backward compatibility: the Felix framework can start, bundles can be
 * managed, and the OsgiPluginRegistry can discover plugins. Also verifies that
 * a legacy JAR (no OSGi manifest) is now rejected with a fail-fast error
 * pointing the plugin author to the Gradle plugin for baking OSGi manifests.
 */
class BackwardCompatibilityTest {

    private FelixPluginManager felixPluginManager;

    @TempDir
    File tempDir;

    @BeforeEach
    void setUp() throws BundleException {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (felixPluginManager != null) {
            felixPluginManager.close();
        }
    }

    @Test
    void felix_framework_starts_cleanly() {
        assertThat(felixPluginManager.isActive(), is(true));
    }

    @Test
    void osgi_registry_returns_empty_when_no_bundles_installed() {
        final BundleContext ctx = felixPluginManager.getBundleContext();
        final OsgiPluginRegistry registry = new OsgiPluginRegistry(ctx);

        assertThat(registry.getPluginCount(), is(0));
    }

    @Test
    void osgi_registry_findPluginClass_returns_empty_for_unknown_plugin() {
        final BundleContext ctx = felixPluginManager.getBundleContext();
        final OsgiPluginRegistry registry = new OsgiPluginRegistry(ctx);

        assertThat(registry.findPluginClass(Object.class, "nonexistent").isPresent(), is(false));
    }

    @Test
    void osgi_registry_findPluginClasses_returns_empty_for_unknown_type() {
        final BundleContext ctx = felixPluginManager.getBundleContext();
        final OsgiPluginRegistry registry = new OsgiPluginRegistry(ctx);

        assertThat(registry.findPluginClasses(Object.class).isEmpty(), is(true));
    }

    @Test
    void system_bundle_is_always_present() {
        final BundleContext ctx = felixPluginManager.getBundleContext();
        final Bundle systemBundle = ctx.getBundle(0);
        assertThat(systemBundle, is(notNullValue()));
        assertThat(systemBundle.getState(), is(Bundle.ACTIVE));
    }

    /**
     * Verifies that a legacy plugin JAR (no OSGi manifest) is now rejected with
     * a fail-fast BundleLoadException pointing the author to the Gradle plugin.
     * Runtime adaptation has been removed — plugins must arrive already-OSGi.
     */
    @Test
    void legacy_jar_without_osgi_manifest_fails_fast_with_actionable_message() throws Exception {
        final BundleContext ctx = felixPluginManager.getBundleContext();
        final StaticBundleLoader loader = new StaticBundleLoader(ctx, new SimpleMeterRegistry());

        // Build a legacy JAR containing TestLegacyProcessor (no Bundle-SymbolicName)
        final File legacyJar = buildLegacyPluginJar();

        // Loading should fail fast with a clear message
        final BundleLoadException ex = assertThrows(BundleLoadException.class,
                () -> loader.loadBundles(Collections.singletonList(legacyJar)));

        assertThat(ex.getMessage(), containsString("not an OSGi bundle"));
        assertThat(ex.getMessage(), containsString("missing Bundle-SymbolicName"));
        assertThat(ex.getMessage(), containsString("org.opensearch.dataprepper.osgi"));
    }

    /**
     * Builds a legacy plugin JAR (no OSGi manifest headers) that contains:
     * - The compiled TestLegacyProcessor class bytecode
     * - A META-INF/data-prepper.plugins.properties file declaring the package
     */
    private File buildLegacyPluginJar() throws IOException {
        final File jar = new File(tempDir, "test-legacy-plugin.jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        // Intentionally NO Bundle-SymbolicName — this is a legacy JAR

        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            // Add the TestLegacyProcessor class file
            final String classResourcePath = TestLegacyProcessor.class.getName().replace('.', '/') + ".class";
            jos.putNextEntry(new JarEntry(classResourcePath));
            try (InputStream classStream = TestLegacyProcessor.class.getClassLoader()
                    .getResourceAsStream(classResourcePath)) {
                if (classStream == null) {
                    throw new IOException("Cannot find class resource: " + classResourcePath);
                }
                classStream.transferTo(jos);
            }
            jos.closeEntry();

            // Add META-INF/data-prepper.plugins.properties
            jos.putNextEntry(new JarEntry("META-INF/data-prepper.plugins.properties"));
            final Properties props = new Properties();
            props.setProperty("org.opensearch.dataprepper.plugin.packages",
                    TestLegacyProcessor.class.getPackageName());
            props.store(jos, null);
            jos.closeEntry();
        }

        return jar;
    }
}
