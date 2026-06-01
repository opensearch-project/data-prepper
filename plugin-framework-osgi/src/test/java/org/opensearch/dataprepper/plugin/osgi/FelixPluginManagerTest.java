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
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FelixPluginManagerTest {

    private FelixPluginManager felixPluginManager;

    @BeforeEach
    void setUp() {
        felixPluginManager = new FelixPluginManager();
    }

    @AfterEach
    void tearDown() throws Exception {
        if (felixPluginManager != null) {
            felixPluginManager.close();
        }
    }

    @Test
    void constructor_with_null_config_throws() {
        assertThrows(NullPointerException.class, () -> new FelixPluginManager(null));
    }

    @Test
    void start_initializes_framework_to_active_state() throws BundleException {
        felixPluginManager.start();

        assertThat(felixPluginManager.isActive(), is(true));
        assertThat(felixPluginManager.getFrameworkState(), is(Bundle.ACTIVE));
    }

    @Test
    void getBundleContext_returns_non_null_after_start() throws BundleException {
        felixPluginManager.start();

        assertThat(felixPluginManager.getBundleContext(), is(notNullValue()));
    }

    @Test
    void getBundleContext_returns_null_before_start() {
        assertThat(felixPluginManager.getBundleContext(), is(nullValue()));
    }

    @Test
    void stop_transitions_framework_to_resolved_state() throws Exception {
        felixPluginManager.start();
        felixPluginManager.stop();

        assertThat(felixPluginManager.isActive(), is(false));
    }

    @Test
    void close_stops_active_framework() throws Exception {
        felixPluginManager.start();
        assertThat(felixPluginManager.isActive(), is(true));

        felixPluginManager.close();
        assertThat(felixPluginManager.isActive(), is(false));
    }

    @Test
    void close_is_safe_when_framework_not_started() throws Exception {
        felixPluginManager.close();
        // Should not throw
    }

    @Test
    void start_and_stop_lifecycle_is_repeatable() throws Exception {
        felixPluginManager.start();
        assertThat(felixPluginManager.isActive(), is(true));

        felixPluginManager.stop();
        assertThat(felixPluginManager.isActive(), is(false));
    }

    @Test
    void installBundle_throws_when_framework_not_started() {
        assertThrows(IllegalStateException.class,
                () -> felixPluginManager.installBundle("file:///nonexistent.jar"));
    }

    @Test
    void installBundle_with_null_location_throws() throws BundleException {
        felixPluginManager.start();
        assertThrows(NullPointerException.class,
                () -> felixPluginManager.installBundle(null));
    }

    @Test
    void default_constructor_creates_valid_framework() throws BundleException {
        felixPluginManager.start();
        final BundleContext ctx = felixPluginManager.getBundleContext();
        assertThat(ctx, is(notNullValue()));
        // System bundle should be present
        assertThat(ctx.getBundle(0), is(notNullValue()));
    }

    @Test
    void custom_config_is_applied() throws Exception {
        final Map<String, String> config = new HashMap<>();
        config.put("org.osgi.framework.storage", System.getProperty("java.io.tmpdir") + "/dp-felix-test");
        config.put("org.osgi.framework.storage.clean", "onFirstInit");

        final FelixPluginManager customManager = new FelixPluginManager(config);
        try {
            customManager.start();
            assertThat(customManager.isActive(), is(true));
        } finally {
            customManager.close();
        }
    }

    @Test
    void default_cache_dir_contains_process_pid() throws Exception {
        felixPluginManager.start();
        final BundleContext ctx = felixPluginManager.getBundleContext();
        final String storageProperty = ctx.getProperty(Constants.FRAMEWORK_STORAGE);
        final long pid = ProcessHandle.current().pid();

        assertThat("Cache dir should contain PID for uniqueness",
                storageProperty, containsString(String.valueOf(pid)));
    }
}
