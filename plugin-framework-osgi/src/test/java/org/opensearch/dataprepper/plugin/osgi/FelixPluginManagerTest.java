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
import org.osgi.framework.Constants;

import java.io.File;
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

    @TempDir
    File tempDir;

    @BeforeEach
    void setUp() {
        // Clear the data-prepper.dir property to ensure test isolation
        System.clearProperty(FelixPluginManager.DATA_PREPPER_DIR_PROPERTY);
    }

    @AfterEach
    void tearDown() throws Exception {
        if (felixPluginManager != null) {
            felixPluginManager.close();
        }
        System.clearProperty(FelixPluginManager.DATA_PREPPER_DIR_PROPERTY);
    }

    @Test
    void constructor_with_null_config_throws() {
        assertThrows(NullPointerException.class, () -> new FelixPluginManager(null));
    }

    @Test
    void start_initializes_framework_to_active_state() throws BundleException {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();

        assertThat(felixPluginManager.isActive(), is(true));
        assertThat(felixPluginManager.getFrameworkState(), is(Bundle.ACTIVE));
    }

    @Test
    void getBundleContext_returns_non_null_after_start() throws BundleException {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();

        assertThat(felixPluginManager.getBundleContext(), is(notNullValue()));
    }

    @Test
    void getBundleContext_returns_null_before_start() {
        felixPluginManager = new FelixPluginManager();
        assertThat(felixPluginManager.getBundleContext(), is(nullValue()));
    }

    @Test
    void stop_transitions_framework_to_resolved_state() throws Exception {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        felixPluginManager.stop();

        assertThat(felixPluginManager.isActive(), is(false));
    }

    @Test
    void close_stops_active_framework() throws Exception {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        assertThat(felixPluginManager.isActive(), is(true));

        felixPluginManager.close();
        assertThat(felixPluginManager.isActive(), is(false));
    }

    @Test
    void close_is_safe_when_framework_not_started() throws Exception {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.close();
        // Should not throw
    }

    @Test
    void start_and_stop_lifecycle_is_repeatable() throws Exception {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        assertThat(felixPluginManager.isActive(), is(true));

        felixPluginManager.stop();
        assertThat(felixPluginManager.isActive(), is(false));
    }

    @Test
    void installBundle_throws_when_framework_not_started() {
        felixPluginManager = new FelixPluginManager();
        assertThrows(IllegalStateException.class,
                () -> felixPluginManager.installBundle("file:///nonexistent.jar"));
    }

    @Test
    void installBundle_with_null_location_throws() throws BundleException {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        assertThrows(NullPointerException.class,
                () -> felixPluginManager.installBundle(null));
    }

    @Test
    void default_constructor_creates_valid_framework() throws BundleException {
        felixPluginManager = new FelixPluginManager();
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

        felixPluginManager = new FelixPluginManager(config);
        try {
            felixPluginManager.start();
            assertThat(felixPluginManager.isActive(), is(true));
        } finally {
            felixPluginManager.close();
            felixPluginManager = null;
        }
    }

    @Test
    void default_cache_dir_contains_process_pid() throws Exception {
        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        final BundleContext ctx = felixPluginManager.getBundleContext();
        final String storageProperty = ctx.getProperty(Constants.FRAMEWORK_STORAGE);
        final long pid = ProcessHandle.current().pid();

        assertThat("Cache dir should contain PID for uniqueness",
                storageProperty, containsString(String.valueOf(pid)));
    }

    @Test
    void default_cache_dir_is_under_data_prepper_dir_when_property_set() throws Exception {
        System.setProperty(FelixPluginManager.DATA_PREPPER_DIR_PROPERTY, tempDir.getAbsolutePath());

        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        final BundleContext ctx = felixPluginManager.getBundleContext();
        final String storageProperty = ctx.getProperty(Constants.FRAMEWORK_STORAGE);

        assertThat("Cache dir should be under data-prepper.dir",
                storageProperty, containsString(tempDir.getAbsolutePath()));
        assertThat("Cache dir should contain osgi path segment",
                storageProperty, containsString("data" + File.separator + "osgi" + File.separator + "felix-cache-"));
    }

    @Test
    void default_cache_dir_falls_back_to_tmpdir_when_data_prepper_dir_not_set() throws Exception {
        System.clearProperty(FelixPluginManager.DATA_PREPPER_DIR_PROPERTY);

        felixPluginManager = new FelixPluginManager();
        felixPluginManager.start();
        final BundleContext ctx = felixPluginManager.getBundleContext();
        final String storageProperty = ctx.getProperty(Constants.FRAMEWORK_STORAGE);
        final String tmpDir = System.getProperty("java.io.tmpdir");

        assertThat("Cache dir should fall back to java.io.tmpdir",
                storageProperty, containsString(tmpDir));
    }
}
