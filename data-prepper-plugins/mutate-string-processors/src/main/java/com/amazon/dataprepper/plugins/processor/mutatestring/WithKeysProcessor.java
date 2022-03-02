/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutatestring;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.event.Event;

import java.util.List;

public abstract class WithKeysProcessor extends AbstractStringProcessor {
    private final List<String> keys;

    public WithKeysProcessor(final PluginMetrics pluginMetrics, final WithKeysProcessorConfig config) {
        super(pluginMetrics);
        this.keys = config.getWithKeys();
    }

    @Override
    protected void performStringAction(Event recordEvent) {
        for(String key : keys) {
            final Object value = recordEvent.get(key, Object.class);
            if(value instanceof String) {
                performKeyAction(recordEvent, key, (String) value);
            }
        }
    }

    protected abstract void performKeyAction(Event recordEvent, String key, String value);
}
