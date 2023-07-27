/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser.model;

import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.sink.SinkContext;

import java.util.Map;

public class SinkContextPluginSetting extends PluginSetting {
    private final SinkContext sinkContext;

    public SinkContextPluginSetting(final String name, final Map<String, Object> settings, final SinkContext sinkContext) {
        super(name, settings);
        this.sinkContext = sinkContext;
    }

    public SinkContext getSinkContext() {
        return sinkContext;
    }
}
