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
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.wiring.BundleWiring;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class BundleHealthCheckTest {

    private FelixPluginManager felixPluginManager;
    private BundleContext bundleContext;
    private BundleHealthCheck healthCheck;

    @TempDir
    File tempDir;

    @BeforeEach
    void setUp() throws BundleException {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        bundleContext = felixPluginManager.getBundleContext();
        healthCheck = new BundleHealthCheck(bundleContext);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (felixPluginManager != null) {
            felixPluginManager.close();
        }
    }

    @Test
    void constructor_with_null_throws() {
        assertThrows(NullPointerException.class, () -> new BundleHealthCheck(null));
    }

    @Test
    void isHealthy_returns_true_with_no_bundles() {
        // Only system bundle present — framework is healthy
        assertThat(healthCheck.isHealthy(), is(true));
    }

    @Test
    void isFrameworkActive_returns_true_when_started() {
        assertThat(healthCheck.isFrameworkActive(), is(true));
    }

    @Test
    void getUnhealthyBundles_returns_empty_with_no_bundles() {
        final List<String> unhealthy = healthCheck.getUnhealthyBundles();

        assertThat(unhealthy.isEmpty(), is(true));
    }

    @Test
    void getBundleStatuses_returns_empty_map_with_no_bundles() {
        final Map<String, String> statuses = healthCheck.getBundleStatuses();

        assertThat(statuses, is(notNullValue()));
        // Only system bundle exists (excluded from statuses)
        assertThat(statuses.isEmpty(), is(true));
    }

    @Test
    void isHealthy_returns_true_with_active_bundle() throws Exception {
        installAndStartBundle("healthy-bundle");

        assertThat(healthCheck.isHealthy(), is(true));
        assertThat(healthCheck.getUnhealthyBundles().isEmpty(), is(true));
    }

    @Test
    void getBundleStatuses_includes_installed_bundle() throws Exception {
        installAndStartBundle("status-bundle");

        final Map<String, String> statuses = healthCheck.getBundleStatuses();

        assertThat(statuses, hasKey("test.status-bundle"));
        assertThat(statuses.get("test.status-bundle"), is("ACTIVE"));
    }

    @Test
    void getUnhealthyBundles_detects_installed_not_resolved_bundle() throws Exception {
        // Install a bundle with unsatisfied imports — it will stay INSTALLED
        final File jar = createUnresolvableBundle("unresolved-bundle");
        bundleContext.installBundle(jar.toURI().toString());

        final List<String> unhealthy = healthCheck.getUnhealthyBundles();

        assertThat(unhealthy.contains("test.unresolved-bundle"), is(true));
    }

    @Test
    void isHealthy_returns_false_with_unresolved_bundle() throws Exception {
        final File jar = createUnresolvableBundle("broken-bundle");
        bundleContext.installBundle(jar.toURI().toString());

        assertThat(healthCheck.isHealthy(), is(false));
    }

    @Test
    void isClassloaderIsolationPresent_returns_true_for_active_bundles() throws Exception {
        installAndStartBundle("isolated-cl-bundle");

        assertThat(healthCheck.isClassloaderIsolationPresent(), is(true));
    }

    @Test
    void isClassloaderIsolationPresent_returns_true_with_no_resolved_bundles() {
        // Only system bundle — no other bundles to check
        assertThat(healthCheck.isClassloaderIsolationPresent(), is(true));
    }

    @Test
    void isClassloaderIsolationPresent_returns_false_when_a_bundle_has_no_wiring() {
        final Bundle bundle = activeNonSystemBundle("test.no-wiring-bundle");
        when(bundle.adapt(BundleWiring.class)).thenReturn(null);
        final BundleHealthCheck mockedHealthCheck = healthCheckOver(bundle);

        assertThat(mockedHealthCheck.isClassloaderIsolationPresent(), is(false));
        assertThat(mockedHealthCheck.isHealthy(), is(false));
    }

    @Test
    void isClassloaderIsolationPresent_returns_false_when_adapting_a_bundle_to_its_wiring_throws() {
        final Bundle bundle = activeNonSystemBundle("test.adapt-throws-bundle");
        when(bundle.adapt(BundleWiring.class)).thenThrow(new IllegalStateException("adapt refused"));
        final BundleHealthCheck mockedHealthCheck = healthCheckOver(bundle);

        assertThat(mockedHealthCheck.isClassloaderIsolationPresent(), is(false));
        assertThat(mockedHealthCheck.isHealthy(), is(false));
    }

    @Test
    void isClassloaderIsolationPresent_returns_false_when_reading_a_bundle_classloader_throws() {
        final Bundle bundle = activeNonSystemBundle("test.classloader-throws-bundle");
        final BundleWiring wiring = mock(BundleWiring.class);
        when(wiring.getClassLoader()).thenThrow(new IllegalStateException("classloader unavailable"));
        when(bundle.adapt(BundleWiring.class)).thenReturn(wiring);
        final BundleHealthCheck mockedHealthCheck = healthCheckOver(bundle);

        assertThat(mockedHealthCheck.isClassloaderIsolationPresent(), is(false));
        assertThat(mockedHealthCheck.isHealthy(), is(false));
    }

    /**
     * Builds a health check over a mocked framework holding an ACTIVE system bundle and the given
     * non-system bundle. Every structural check other than classloader isolation passes for such a
     * framework, so {@code isHealthy()} reflects the isolation verdict alone.
     *
     * @param nonSystemBundle the single non-system bundle the framework reports
     * @return the health check under test
     */
    private static BundleHealthCheck healthCheckOver(final Bundle nonSystemBundle) {
        final Bundle systemBundle = mock(Bundle.class);
        when(systemBundle.getBundleId()).thenReturn(0L);
        when(systemBundle.getState()).thenReturn(Bundle.ACTIVE);

        final BundleContext mockedContext = mock(BundleContext.class);
        when(mockedContext.getBundle(0)).thenReturn(systemBundle);
        when(mockedContext.getBundles()).thenReturn(new Bundle[] {systemBundle, nonSystemBundle});
        return new BundleHealthCheck(mockedContext);
    }

    private static Bundle activeNonSystemBundle(final String symbolicName) {
        final Bundle bundle = mock(Bundle.class);
        when(bundle.getBundleId()).thenReturn(1L);
        when(bundle.getState()).thenReturn(Bundle.ACTIVE);
        when(bundle.getSymbolicName()).thenReturn(symbolicName);
        return bundle;
    }

    private void installAndStartBundle(final String name) throws Exception {
        final File jar = createValidBundle(name);
        final Bundle bundle = bundleContext.installBundle(jar.toURI().toString());
        bundle.start();
    }

    private File createValidBundle(final String name) throws IOException {
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

    private File createUnresolvableBundle(final String name) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Bundle-SymbolicName", "test." + name);
        manifest.getMainAttributes().putValue("Bundle-Version", "1.0.0");
        manifest.getMainAttributes().putValue("Import-Package",
                "com.totally.nonexistent.package;version=\"[50.0.0,51.0.0)\"");
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            // Empty bundle with unsatisfiable import
        }
        return jar;
    }
}
