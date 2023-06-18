/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.model;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.sink.SinkContext;

import java.util.Collection;
import java.util.Map;

public class RoutedPluginSetting extends PluginSetting {
    private final SinkContext sinkContext;

    public RoutedPluginSetting(final String name, final Map<String, Object> settings, final SinkContext sinkContext) {
        super(name, settings);
        this.sinkContext = sinkContext;
    }

    public SinkContext getSinkContext() {
        return sinkContext;
    }
}
