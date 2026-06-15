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
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.plugin.osgi.fixture.TestLegacyProcessor;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies backward compatibility: the Felix framework can start, bundles can be
 * managed, and the OsgiPluginRegistry can discover legacy plugins loaded through
 * the BundleAdapter path — proving that a legacy JAR with no OSGi manifest is
 * auto-wrapped and its {@code @DataPrepperPlugin} annotated class is discoverable
 * by the same registry API used in production.
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
    void bundle_adapter_can_be_created_with_active_context() {
        final BundleContext ctx = felixPluginManager.getBundleContext();
        final BundleAdapter adapter = new BundleAdapter(ctx);
        assertThat(adapter, is(notNullValue()));
    }

    @Test
    void system_bundle_is_always_present() {
        final BundleContext ctx = felixPluginManager.getBundleContext();
        final Bundle systemBundle = ctx.getBundle(0);
        assertThat(systemBundle, is(notNullValue()));
        assertThat(systemBundle.getState(), is(Bundle.ACTIVE));
    }

    /**
     * Verifies that a legacy plugin JAR (no OSGi manifest) containing a class
     * annotated with {@code @DataPrepperPlugin} is discoverable via the
     * OsgiPluginRegistry after being loaded through the BundleAdapter path.
     * This asserts the same contract as the classpath-based legacy discovery:
     * given a plugin name and type, the registry returns the expected class.
     */
    @Test
    void osgi_registry_discovers_legacy_plugin_loaded_through_adapter() throws Exception {
        final BundleContext ctx = felixPluginManager.getBundleContext();
        final BundleAdapter adapter = new BundleAdapter(ctx);
        final StaticBundleLoader loader = new StaticBundleLoader(ctx, adapter, new SimpleMeterRegistry());

        // Build a legacy JAR containing TestLegacyProcessor (no Bundle-SymbolicName)
        final File legacyJar = buildLegacyPluginJar();

        // Load through the production path (adapter wraps it as an OSGi bundle)
        final List<Bundle> loaded = loader.loadBundles(Collections.singletonList(legacyJar));
        assertThat("Legacy JAR should be loaded as a bundle", loaded.size(), is(1));
        assertThat("Bundle should be ACTIVE", loaded.get(0).getState(), is(Bundle.ACTIVE));

        // Discover via OsgiPluginRegistry — the same API used in production
        final OsgiPluginRegistry registry = new OsgiPluginRegistry(ctx);
        registry.scanServices();

        final Optional<? extends Class<? extends Processor>> found =
                registry.findPluginClass(Processor.class, "test_legacy_processor");

        assertTrue(found.isPresent(),
                "OsgiPluginRegistry must discover the legacy plugin 'test_legacy_processor' by name and type");
        assertThat("Discovered class name must match the fixture",
                found.get().getName(), is(TestLegacyProcessor.class.getName()));
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
