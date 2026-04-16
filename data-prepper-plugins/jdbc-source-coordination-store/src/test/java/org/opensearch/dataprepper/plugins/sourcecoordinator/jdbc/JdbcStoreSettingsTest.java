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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcStoreSettingsTest {

    @Test
    void constructor_with_required_fields() {
        final JdbcStoreSettings settings = new JdbcStoreSettings(
                "jdbc:postgresql://localhost/db", "user", "pass",
                null, null, null, null, null);

        assertThat(settings.getUrl(), equalTo("jdbc:postgresql://localhost/db"));
        assertThat(settings.getUsername(), equalTo("user"));
        assertThat(settings.getPassword(), equalTo("pass"));
        assertThat(settings.getTableName(), equalTo("source_coordination"));
        assertThat(settings.skipTableCreation(), is(false));
        assertThat(settings.getMaxPoolSize(), equalTo(5));
        assertThat(settings.getTtl(), is(nullValue()));
        assertThat(settings.getConnectionProperties(), is(nullValue()));
    }

    @Test
    void constructor_with_all_fields() {
        final JdbcStoreSettings settings = new JdbcStoreSettings(
                "jdbc:mysql://localhost/db", "admin", "secret",
                "custom_table", true, 10, Duration.ofHours(24),
                Map.of("ssl", "true"));

        assertThat(settings.getTableName(), equalTo("custom_table"));
        assertThat(settings.skipTableCreation(), is(true));
        assertThat(settings.getMaxPoolSize(), equalTo(10));
        assertThat(settings.getTtl(), equalTo(Duration.ofHours(24)));
        assertThat(settings.getConnectionProperties(), equalTo(Map.of("ssl", "true")));
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
