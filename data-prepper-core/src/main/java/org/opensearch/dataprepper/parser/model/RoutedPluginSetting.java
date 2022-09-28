/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.model;

import com.amazon.dataprepper.model.configuration.PluginSetting;

import java.util.Collection;
import java.util.Map;

public class RoutedPluginSetting extends PluginSetting {
    private final Collection<String> routes;

    public RoutedPluginSetting(final String name, final Map<String, Object> settings, final Collection<String> routes) {
        super(name, settings);
        this.routes = routes;
    }

    public Collection<String> getRoutes() {
        return routes;
    }
}
