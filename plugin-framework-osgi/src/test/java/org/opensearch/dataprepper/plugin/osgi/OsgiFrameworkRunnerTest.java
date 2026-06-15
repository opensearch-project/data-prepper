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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugin.PluginProviderLoader;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(MockitoExtension.class)
class OsgiFrameworkRunnerTest {

    @Mock
    private PluginProviderLoader pluginProviderLoader;

    private OsgiFrameworkRunner runner;

    @TempDir
    File tempDir;

    @AfterEach
    void tearDown() {
        if (runner != null) {
            runner.shutdown();
        }
        System.clearProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY);
        System.clearProperty(OsgiFrameworkRunner.BUNDLES_DIR_PROPERTY);
    }

    @Test
    void initialize_in_legacy_mode_does_not_start_felix() {
        System.setProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY, "legacy");

        runner = new OsgiFrameworkRunner(pluginProviderLoader);
        runner.initialize();

        assertThat(runner.isActive(), is(false));
        assertThat(runner.getBundleHealthCheck(), is(nullValue()));
    }

    @Test
    void initialize_in_osgi_mode_starts_felix() {
        System.setProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY, "osgi");

        runner = new OsgiFrameworkRunner(pluginProviderLoader);
        runner.initialize();

        assertThat(runner.isActive(), is(true));
        assertThat(runner.getBundleHealthCheck(), is(notNullValue()));
    }

    @Test
    void initialize_in_osgi_mode_without_bundles_dir_starts_without_bundles() {
        System.setProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY, "osgi");
        System.clearProperty(OsgiFrameworkRunner.BUNDLES_DIR_PROPERTY);

        runner = new OsgiFrameworkRunner(pluginProviderLoader);
        runner.initialize();

        assertThat(runner.isActive(), is(true));
        assertThat(runner.getBundleHealthCheck().isHealthy(), is(true));
    }

    @Test
    void initialize_in_osgi_mode_with_valid_bundles_dir_loads_bundles() throws Exception {
        System.setProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY, "osgi");
        System.setProperty(OsgiFrameworkRunner.BUNDLES_DIR_PROPERTY, tempDir.getAbsolutePath());
        createValidBundle("runner-test-bundle");

        runner = new OsgiFrameworkRunner(pluginProviderLoader);
        runner.initialize();

        assertThat(runner.isActive(), is(true));
        assertThat(runner.getBundleHealthCheck().isHealthy(), is(true));
    }

    @Test
    void initialize_in_osgi_mode_with_bad_bundle_fails_fast() throws Exception {
        System.setProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY, "osgi");
        System.setProperty(OsgiFrameworkRunner.BUNDLES_DIR_PROPERTY, tempDir.getAbsolutePath());
        createUnresolvableBundle("unresolvable-runner");

        runner = new OsgiFrameworkRunner(pluginProviderLoader);

        assertThrows(RuntimeException.class, () -> runner.initialize());
    }

    @Test
    void shutdown_stops_active_framework() {
        System.setProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY, "osgi");

        runner = new OsgiFrameworkRunner(pluginProviderLoader);
        runner.initialize();
        assertThat(runner.isActive(), is(true));

        runner.shutdown();
        assertThat(runner.isActive(), is(false));
    }

    @Test
    void shutdown_is_safe_when_not_initialized() {
        runner = new OsgiFrameworkRunner(pluginProviderLoader);
        runner.shutdown();
        // Should not throw
    }

    @Test
    void isOsgiMode_returns_false_by_default() {
        System.clearProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY);
        assertThat(OsgiFrameworkRunner.isOsgiMode(), is(false));
    }

    private void createValidBundle(final String name) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Bundle-SymbolicName", "test." + name);
        manifest.getMainAttributes().putValue("Bundle-Version", "1.0.0");
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            // Empty bundle
        }
    }

    private void createUnresolvableBundle(final String name) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Bundle-SymbolicName", "test." + name);
        manifest.getMainAttributes().putValue("Bundle-Version", "1.0.0");
        manifest.getMainAttributes().putValue("Import-Package",
                "com.absolutely.nonexistent;version=\"[99.0,100.0)\"");
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            // Empty with bad imports
        }
    }
}
