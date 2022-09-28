/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

class RoutedPluginSettingTest {
    private String name;
    private Map<String, Object> settings;
    private Collection<String> routes;

    @BeforeEach
    void setUp() {
        name = UUID.randomUUID().toString();
        settings = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        routes = Set.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());

    }

    private RoutedPluginSetting createObjectUnderTest() {
        return new RoutedPluginSetting(name, settings, routes);
    }

    @Test
    void getName_returns_name_from_constructor() {
        assertThat(createObjectUnderTest().getName(), equalTo(name));
    }

    @Test
    void getSettings_returns_settings_from_constructor() {
        assertThat(createObjectUnderTest().getSettings(), equalTo(settings));
    }

    @Test
    void getRoutes_returns_routes_from_constructor() {
        assertThat(createObjectUnderTest().getRoutes(), equalTo(routes));
    }
}