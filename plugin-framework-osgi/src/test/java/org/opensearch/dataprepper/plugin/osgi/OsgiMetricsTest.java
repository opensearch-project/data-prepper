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
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.osgi.framework.BundleException;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Verifies that OSGi metrics are properly registered and emitted via Micrometer
 * using Data Prepper's convention (Metrics.globalRegistry).
 */
class OsgiMetricsTest {

    private FelixPluginManager felixPluginManager;
    private MeterRegistry meterRegistry;

    @TempDir
    File tempDir;

    @BeforeEach
    void setUp() throws BundleException {
        meterRegistry = new SimpleMeterRegistry();

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
    void bundlesLoaded_counter_is_registered_with_expected_name() throws Exception {
        final StaticBundleLoader loader = new StaticBundleLoader(felixPluginManager.getBundleContext(), meterRegistry);
        final File jar = createBundle("counter-test");

        loader.loadBundles(Collections.singletonList(jar));

        final Counter counter = meterRegistry.find(StaticBundleLoader.METRIC_BUNDLES_LOADED).counter();
        assertThat(counter, is(notNullValue()));
        assertThat(counter.count(), greaterThanOrEqualTo(1.0));
    }

    @Test
    void bundlesFailed_counter_is_registered_with_expected_name() throws Exception {
        final StaticBundleLoader loader = new StaticBundleLoader(felixPluginManager.getBundleContext(), meterRegistry);
        final File jar = createUnresolvableBundle("failed-counter-test");

        try {
            loader.loadBundles(Collections.singletonList(jar));
        } catch (final BundleLoadException ignored) {
            // expected
        }

        final Counter counter = meterRegistry.find(StaticBundleLoader.METRIC_BUNDLES_FAILED).counter();
        assertThat(counter, is(notNullValue()));
        assertThat(counter.count(), greaterThanOrEqualTo(1.0));
    }

    @Test
    void resolutionDuration_timer_records_time() throws Exception {
        final StaticBundleLoader loader = new StaticBundleLoader(felixPluginManager.getBundleContext(), meterRegistry);
        final File jar = createBundle("timer-test");

        loader.loadBundles(Collections.singletonList(jar));

        final Timer timer = meterRegistry.find(StaticBundleLoader.METRIC_RESOLUTION_DURATION).timer();
        assertThat(timer, is(notNullValue()));
        assertThat(timer.count(), is(1L));
        assertThat(timer.totalTime(java.util.concurrent.TimeUnit.NANOSECONDS), greaterThan(0.0));
    }

    @Test
    void bundlesActive_gauge_is_registered() throws Exception {
        final StaticBundleLoader loader = new StaticBundleLoader(felixPluginManager.getBundleContext(), meterRegistry);
        final File jar = createBundle("gauge-test");

        loader.loadBundles(Collections.singletonList(jar));

        final Gauge gauge = meterRegistry.find(StaticBundleLoader.METRIC_BUNDLES_ACTIVE).gauge();
        assertThat(gauge, is(notNullValue()));
        assertThat((int) gauge.value(), greaterThanOrEqualTo(1));
    }

    @Test
    void multiple_instances_with_separate_registries_do_not_collide() throws Exception {
        final MeterRegistry registry1 = new SimpleMeterRegistry();
        final MeterRegistry registry2 = new SimpleMeterRegistry();

        final StaticBundleLoader loader1 = new StaticBundleLoader(felixPluginManager.getBundleContext(), registry1);
        final StaticBundleLoader loader2 = new StaticBundleLoader(felixPluginManager.getBundleContext(), registry2);

        final File jar = createBundle("multi-instance-test");
        loader1.loadBundles(Collections.singletonList(jar));

        final Counter counter1 = registry1.find(StaticBundleLoader.METRIC_BUNDLES_LOADED).counter();
        final Counter counter2 = registry2.find(StaticBundleLoader.METRIC_BUNDLES_LOADED).counter();

        assertThat(counter1, is(notNullValue()));
        assertThat(counter1.count(), greaterThanOrEqualTo(1.0));
        // loader2 was never used so its counter should be 0
        assertThat(counter2, is(notNullValue()));
        assertThat(counter2.count(), is(0.0));
    }

    @Test
    void metric_names_follow_osgi_plugin_prefix_convention() {
        assertThat(StaticBundleLoader.METRIC_BUNDLES_LOADED.startsWith("osgi.plugin."), is(true));
        assertThat(StaticBundleLoader.METRIC_BUNDLES_FAILED.startsWith("osgi.plugin."), is(true));
        assertThat(StaticBundleLoader.METRIC_RESOLUTION_DURATION.startsWith("osgi.plugin."), is(true));
        assertThat(StaticBundleLoader.METRIC_BUNDLES_ACTIVE.startsWith("osgi.plugin."), is(true));
    }

    private File createBundle(final String name) throws IOException {
        final File jar = new File(tempDir, name + ".jar");
        final Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Bundle-SymbolicName", "test." + name);
        manifest.getMainAttributes().putValue("Bundle-Version", "1.0.0");
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            // Empty
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
                "com.does.not.exist;version=\"[99.0,100.0)\"");
        try (JarOutputStream jos = new JarOutputStream(new FileOutputStream(jar), manifest)) {
            // Empty with bad import
        }
        return jar;
    }
}
