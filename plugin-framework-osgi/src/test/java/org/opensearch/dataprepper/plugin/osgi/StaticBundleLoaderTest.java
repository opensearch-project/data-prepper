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

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StaticBundleLoaderTest {

    private FelixPluginManager felixPluginManager;
    private StaticBundleLoader loader;
    private MeterRegistry meterRegistry;

    @TempDir
    File tempDir;

    @BeforeEach
    void setUp() throws BundleException {
        meterRegistry = new SimpleMeterRegistry();

        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        loader = new StaticBundleLoader(felixPluginManager.getBundleContext(), meterRegistry);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (felixPluginManager != null) {
            felixPluginManager.close();
        }
    }

    @Test
    void loadBundles_with_empty_directory_returns_empty_list() {
        final List<Bundle> result = loader.loadBundles(tempDir);

        assertThat(result.isEmpty(), is(true));
    }

    @Test
    void loadBundles_with_nonexistent_directory_throws() {
        final File noDir = new File(tempDir, "nonexistent");

        final BundleLoadException ex = assertThrows(BundleLoadException.class,
                () -> loader.loadBundles(noDir));

        assertThat(ex.getMessage(), containsString("does not exist"));
    }

    @Test
    void loadBundles_with_null_directory_throws() {
        assertThrows(NullPointerException.class, () -> loader.loadBundles((File) null));
    }

    @Test
    void loadBundles_with_valid_bundle_jar_succeeds() throws Exception {
        final File jar = createValidBundle("static-test-bundle", "1.0.0");

        final List<Bundle> result = loader.loadBundles(Collections.singletonList(jar));

        assertThat(result.size(), is(1));
        assertThat(result.get(0).getState(), is(Bundle.ACTIVE));
        assertThat(result.get(0).getSymbolicName(), is("test.static-test-bundle"));
    }

    @Test
    void loadBundles_installs_multiple_bundles() throws Exception {
        final File jar1 = createValidBundle("multi-a", "1.0.0");
        final File jar2 = createValidBundle("multi-b", "1.0.0");

        final List<Bundle> result = loader.loadBundles(Arrays.asList(jar1, jar2));

        assertThat(result.size(), is(2));
        for (final Bundle b : result) {
            assertThat(b.getState(), is(Bundle.ACTIVE));
        }
    }

    @Test
    void loadBundles_from_directory_finds_jar_files() throws Exception {
        createValidBundleInDir("dir-bundle-a", "1.0.0");
        createValidBundleInDir("dir-bundle-b", "1.0.0");
        // Create a non-jar file that should be ignored
        new File(tempDir, "readme.txt").createNewFile();

        final List<Bundle> result = loader.loadBundles(tempDir);

        assertThat(result.size(), is(2));
    }

    @Test
    void loadBundles_with_unresolvable_bundle_throws_with_translated_message() throws Exception {
        // Create a bundle that imports a non-existent package
        final File jar = createUnresolvableBundle("bad-import-bundle");

        final BundleLoadException ex = assertThrows(BundleLoadException.class,
                () -> loader.loadBundles(Collections.singletonList(jar)));

        assertThat(ex.getMessage(), is(notNullValue()));
        assertThat(ex.getMessage(), containsString("resolution failed"));
    }

    @Test
    void loadBundles_with_empty_list_returns_empty() {
        final List<Bundle> result = loader.loadBundles(Collections.emptyList());

        assertThat(result.isEmpty(), is(true));
    }

    @Test
    void metrics_are_registered_after_successful_load() throws Exception {
        final File jar = createValidBundle("metrics-test", "1.0.0");

        loader.loadBundles(Collections.singletonList(jar));

        final Counter loaded = meterRegistry.find(StaticBundleLoader.METRIC_BUNDLES_LOADED).counter();
        assertThat(loaded, is(notNullValue()));
        assertThat(loaded.count(), greaterThanOrEqualTo(1.0));

        final Timer timer = meterRegistry.find(StaticBundleLoader.METRIC_RESOLUTION_DURATION).timer();
        assertThat(timer, is(notNullValue()));
        assertThat(timer.count(), greaterThanOrEqualTo(1L));
    }

    @Test
    void metrics_bundles_failed_increments_on_resolution_failure() throws Exception {
        final File jar = createUnresolvableBundle("fail-metric-test");

        try {
            loader.loadBundles(Collections.singletonList(jar));
        } catch (final BundleLoadException ignored) {
            // expected
        }

        final Counter failed = meterRegistry.find(StaticBundleLoader.METRIC_BUNDLES_FAILED).counter();
        assertThat(failed, is(notNullValue()));
        assertThat(failed.count(), greaterThanOrEqualTo(1.0));
    }

    @Test
    void loadBundles_with_non_osgi_jar_throws_fail_fast_with_actionable_message() throws Exception {
        final File jar = createLegacyJar("legacy-plugin");

        final BundleLoadException ex = assertThrows(BundleLoadException.class,
                () -> loader.loadBundles(Collections.singletonList(jar)));

        assertThat(ex.getMessage(), containsString("legacy-plugin.jar"));
        assertThat(ex.getMessage(), containsString("not an OSGi bundle"));
        assertThat(ex.getMessage(), containsString("missing Bundle-SymbolicName"));
        assertThat(ex.getMessage(), containsString("org.opensearch.dataprepper.osgi"));
        assertThat(ex.getMessage(), containsString("Gradle plugin"));
    }

    @Test
    void loadBundles_with_non_osgi_jar_increments_failed_counter() throws Exception {
        final File jar = createLegacyJar("legacy-counter-test");

        try {
            loader.loadBundles(Collections.singletonList(jar));
        } catch (final BundleLoadException ignored) {
            // expected
        }

        final Counter failed = meterRegistry.find(StaticBundleLoader.METRIC_BUNDLES_FAILED).counter();
        assertThat(failed, is(notNullValue()));
        assertThat(failed.count(), greaterThanOrEqualTo(1.0));
    }

    @Test
    void loadBundles_with_osgi_jar_installs_directly() throws Exception {
        final File jar = createValidBundle("already-osgi-bundle", "1.0.0");

        final List<Bundle> result = loader.loadBundles(Collections.singletonList(jar));

        assertThat(result.size(), is(1));
        assertThat(result.get(0).getState(), is(Bundle.ACTIVE));
        // OSGi jars retain their original symbolic name
        assertThat(result.get(0).getSymbolicName(), is("test.already-osgi-bundle"));
    }

    @Test
    void getStateString_returns_expected_values() {
        assertThat(StaticBundleLoader.getStateString(Bundle.UNINSTALLED), is("UNINSTALLED"));
        assertThat(StaticBundleLoader.getStateString(Bundle.INSTALLED), is("INSTALLED"));
        assertThat(StaticBundleLoader.getStateString(Bundle.RESOLVED), is("RESOLVED"));
        assertThat(StaticBundleLoader.getStateString(Bundle.STARTING), is("STARTING"));
        assertThat(StaticBundleLoader.getStateString(Bundle.STOPPING), is("STOPPING"));
        assertThat(StaticBundleLoader.getStateString(Bundle.ACTIVE), is("ACTIVE"));
        assertThat(StaticBundleLoader.getStateString(999), is("UNKNOWN(999)"));
    }

    private File createLegacyJar(final String name) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        // No Bundle-SymbolicName — this is a legacy/plain JAR, not an OSGi bundle
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            // Empty JAR with just a manifest
        }
        return jar;
    }

    private File createValidBundle(final String name, final String version) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Bundle-SymbolicName", "test." + name);
        manifest.getMainAttributes().putValue("Bundle-Version", version);
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            // Empty bundle — valid for install/resolve/start
        }
        return jar;
    }

    private File createValidBundleInDir(final String name, final String version) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Bundle-SymbolicName", "test." + name);
        manifest.getMainAttributes().putValue("Bundle-Version", version);
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            // Empty bundle
        }
        return jar;
    }

    private File createUnresolvableBundle(final String name) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Bundle-SymbolicName", "test." + name);
        manifest.getMainAttributes().putValue("Bundle-Version", "1.0.0");
        // Import a package that does not exist in the framework
        manifest.getMainAttributes().putValue("Import-Package",
                "com.nonexistent.package.that.does.not.exist;version=\"[99.0.0,100.0.0)\"");
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            // Empty bundle with unsatisfiable import
        }
        return jar;
    }
}
