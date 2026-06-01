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
import org.osgi.framework.BundleException;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

class OsgiPluginRegistryTest {

    private FelixPluginManager felixPluginManager;
    private OsgiPluginRegistry registry;

    @BeforeEach
    void setUp() throws BundleException {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        registry = new OsgiPluginRegistry(felixPluginManager.getBundleContext());
    }

    @AfterEach
    void tearDown() throws Exception {
        if (felixPluginManager != null) {
            felixPluginManager.close();
        }
    }

    @Test
    void scanServices_with_no_services_produces_empty_map() {
        registry.scanServices();
        assertThat(registry.getPluginCount(), is(0));
    }

    @Test
    void findPluginClass_returns_empty_for_unknown_plugin() {
        assertThat(registry.findPluginClass(Object.class, "unknown").isPresent(), is(false));
    }

    @Test
    void findPluginClasses_returns_empty_for_unknown_type() {
        assertThat(registry.findPluginClasses(Object.class).isEmpty(), is(true));
    }

    @Test
    void getPluginCount_triggers_scan_on_first_call() {
        // First call should trigger scan
        final int count = registry.getPluginCount();
        assertThat(count, is(0));
        // Second call should use cached result
        assertThat(registry.getPluginCount(), is(0));
    }

    @Test
    void findPluginClass_triggers_scan_on_first_call() {
        registry.findPluginClass(Object.class, "test");
        // Should not throw, scan happens lazily
        assertThat(registry.getPluginCount(), is(0));
    }

    @Test
    void findPluginClasses_triggers_scan_on_first_call() {
        registry.findPluginClasses(Object.class);
        assertThat(registry.getPluginCount(), is(0));
    }

    @Test
    void refresh_invalidates_cache_and_next_call_rescans() {
        // First access triggers scan
        assertThat(registry.getPluginCount(), is(0));
        // Refresh clears cached state
        registry.refresh();
        // Next call should re-scan without error
        assertThat(registry.getPluginCount(), is(0));
    }

    @Test
    void refresh_is_thread_safe_with_concurrent_readers() throws Exception {
        // Trigger initial scan
        registry.getPluginCount();

        final int threadCount = 10;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicBoolean failed = new AtomicBoolean(false);
        final ExecutorService executor = Executors.newFixedThreadPool(threadCount);

        for (int i = 0; i < threadCount; i++) {
            final int index = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    if (index % 2 == 0) {
                        registry.refresh();
                    } else {
                        final Optional<?> result = registry.findPluginClass(Object.class, "test");
                        // Should never throw NPE from torn reads
                        assertThat(result.isPresent(), is(false));
                    }
                } catch (final Exception e) {
                    failed.set(true);
                }
            });
        }

        startLatch.countDown();
        executor.shutdown();
        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertThat("Concurrent refresh/read caused failure", failed.get(), is(false));
    }
}
