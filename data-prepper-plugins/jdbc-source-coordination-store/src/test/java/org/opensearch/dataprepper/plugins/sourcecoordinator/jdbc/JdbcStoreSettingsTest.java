/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.sourcecoordinator.jdbc;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcStoreSettingsTest {

    @Test
    void constructor_with_required_fields() {
        final JdbcStoreSettings settings = new JdbcStoreSettings(
                "jdbc:postgresql://localhost/db", "user", "pass",
                null, null, null, null, null);

        assertEquals("jdbc:postgresql://localhost/db", settings.getUrl());
        assertEquals("user", settings.getUsername());
        assertEquals("pass", settings.getPassword());
        assertEquals("source_coordination", settings.getTableName());
        assertFalse(settings.skipTableCreation());
        assertEquals(5, settings.getMaxPoolSize());
        assertNull(settings.getTtl());
        assertNull(settings.getConnectionProperties());
    }

    @Test
    void constructor_with_all_fields() {
        final JdbcStoreSettings settings = new JdbcStoreSettings(
                "jdbc:mysql://localhost/db", "admin", "secret",
                "custom_table", true, 10, Duration.ofHours(24),
                Map.of("ssl", "true"));

        assertEquals("custom_table", settings.getTableName());
        assertTrue(settings.skipTableCreation());
        assertEquals(10, settings.getMaxPoolSize());
        assertEquals(Duration.ofHours(24), settings.getTtl());
        assertEquals(Map.of("ssl", "true"), settings.getConnectionProperties());
    }

    @Test
    void constructor_requires_url() {
        assertThrows(NullPointerException.class, () ->
                new JdbcStoreSettings(null, "user", "pass", null, null, null, null, null));
    }

    @Test
    void constructor_requires_username() {
        assertThrows(NullPointerException.class, () ->
                new JdbcStoreSettings("jdbc:postgresql://localhost/db", null, "pass", null, null, null, null, null));
    }

    @Test
    void constructor_requires_password() {
        assertThrows(NullPointerException.class, () ->
                new JdbcStoreSettings("jdbc:postgresql://localhost/db", "user", null, null, null, null, null, null));
    }
}
