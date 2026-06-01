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
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
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
import java.util.Optional;
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

        runner = new OsgiFrameworkRunner(pluginProviderLoader, Optional.of(new CompositeMeterRegistry()));
        runner.initialize();

        assertThat(runner.isActive(), is(false));
        assertThat(runner.getBundleHealthCheck(), is(nullValue()));
    }

    @Test
    void initialize_in_osgi_mode_starts_felix() {
        System.setProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY, "osgi");

        runner = new OsgiFrameworkRunner(pluginProviderLoader, Optional.of(new CompositeMeterRegistry()));
        runner.initialize();

        assertThat(runner.isActive(), is(true));
        assertThat(runner.getBundleHealthCheck(), is(notNullValue()));
    }

    @Test
    void initialize_in_osgi_mode_without_bundles_dir_starts_without_bundles() {
        System.setProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY, "osgi");
        System.clearProperty(OsgiFrameworkRunner.BUNDLES_DIR_PROPERTY);

        runner = new OsgiFrameworkRunner(pluginProviderLoader, Optional.of(new CompositeMeterRegistry()));
        runner.initialize();

        assertThat(runner.isActive(), is(true));
        assertThat(runner.getBundleHealthCheck().isHealthy(), is(true));
    }

    @Test
    void initialize_in_osgi_mode_with_valid_bundles_dir_loads_bundles() throws Exception {
        System.setProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY, "osgi");
        System.setProperty(OsgiFrameworkRunner.BUNDLES_DIR_PROPERTY, tempDir.getAbsolutePath());
        createValidBundle("runner-test-bundle");

        runner = new OsgiFrameworkRunner(pluginProviderLoader, Optional.of(new CompositeMeterRegistry()));
        runner.initialize();

        assertThat(runner.isActive(), is(true));
        assertThat(runner.getBundleHealthCheck().isHealthy(), is(true));
    }

    /**
     * A child {@link SimpleMeterRegistry} is required because a {@link CompositeMeterRegistry} with no
     * children hands out composite meters that have nothing to delegate to, so the recorded value stays
     * at zero and the count assertion below could never observe the increment.
     * <p>
     * The {@code Metrics.globalRegistry} assertion is the point of this test: it is what fails if OSGi
     * metrics are ever recorded into the global registry — which Data Prepper does not export — instead
     * of the injected registry.
     */
    @Test
    void initialize_records_bundle_metrics_in_the_injected_registry_and_not_the_global_registry() throws Exception {
        System.setProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY, "osgi");
        System.setProperty(OsgiFrameworkRunner.BUNDLES_DIR_PROPERTY, tempDir.getAbsolutePath());
        createValidBundle("injected-registry-bundle");

        final CompositeMeterRegistry registry = new CompositeMeterRegistry();
        registry.add(new SimpleMeterRegistry());

        runner = new OsgiFrameworkRunner(pluginProviderLoader, Optional.of(registry));
        runner.initialize();

        final Counter bundlesLoaded = registry.find(StaticBundleLoader.METRIC_BUNDLES_LOADED).counter();
        assertThat(bundlesLoaded, is(notNullValue()));
        assertThat(bundlesLoaded.count(), is(1.0));

        assertThat(Metrics.globalRegistry.find(StaticBundleLoader.METRIC_BUNDLES_LOADED).counter(), is(nullValue()));
    }

    @Test
    void initialize_in_osgi_mode_with_bad_bundle_fails_fast() throws Exception {
        System.setProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY, "osgi");
        System.setProperty(OsgiFrameworkRunner.BUNDLES_DIR_PROPERTY, tempDir.getAbsolutePath());
        createUnresolvableBundle("unresolvable-runner");

        runner = new OsgiFrameworkRunner(pluginProviderLoader, Optional.of(new CompositeMeterRegistry()));

        assertThrows(RuntimeException.class, () -> runner.initialize());
    }

    @Test
    void shutdown_stops_active_framework() {
        System.setProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY, "osgi");

        runner = new OsgiFrameworkRunner(pluginProviderLoader, Optional.of(new CompositeMeterRegistry()));
        runner.initialize();
        assertThat(runner.isActive(), is(true));

        runner.shutdown();
        assertThat(runner.isActive(), is(false));
    }

    @Test
    void initialize_in_osgi_mode_without_a_meter_registry_bean_still_starts_felix() {
        System.setProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY, "osgi");

        // Several Spring contexts scan the OSGi package without the metrics configuration, so the
        // registry must be optional rather than a required constructor dependency.
        runner = new OsgiFrameworkRunner(pluginProviderLoader, Optional.empty());
        runner.initialize();

        assertThat(runner.isActive(), is(true));
        assertThat(runner.getBundleHealthCheck().isHealthy(), is(true));
    }

    @Test
    void shutdown_is_safe_when_not_initialized() {
        runner = new OsgiFrameworkRunner(pluginProviderLoader, Optional.of(new CompositeMeterRegistry()));
        runner.shutdown();
        // Should not throw
    }

    @Test
    void isOsgiMode_returns_false_by_default() {
        System.clearProperty(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY);
        assertThat(OsgiFrameworkRunner.isOsgiMode(), is(false));
    }

    @Test
    void plugin_framework_property_uses_data_prepper_prefix() {
        assertThat(OsgiFrameworkRunner.PLUGIN_FRAMEWORK_PROPERTY, is("data-prepper.plugin.framework"));
    }

    @Test
    void bundles_dir_property_uses_data_prepper_prefix() {
        assertThat(OsgiFrameworkRunner.BUNDLES_DIR_PROPERTY, is("data-prepper.plugin.bundles.dir"));
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
