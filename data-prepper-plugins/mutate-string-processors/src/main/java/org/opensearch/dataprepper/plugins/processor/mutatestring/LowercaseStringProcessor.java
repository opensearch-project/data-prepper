/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.processor.Processor;

import java.util.Locale;

/**
 * This processor takes in a key and changes its value to a lowercase string. If the value is not a string,
 * no action is performed.
 */
@DataPrepperPlugin(name = "lowercase_string", pluginType = Processor.class, pluginConfigurationType = LowercaseStringProcessorConfig.class)
public class LowercaseStringProcessor extends AbstractStringProcessor<EventKey> {
    @DataPrepperPluginConstructor
    public LowercaseStringProcessor(final PluginMetrics pluginMetrics, final WithKeysConfig config) {
        super(pluginMetrics, config);
    }

    @Override
    protected void performKeyAction(final Event recordEvent, final EventKey key, final String value)
    {
        recordEvent.put(key, value.toLowerCase(Locale.ROOT));
    }

    @Override
    protected EventKey getKey(final EventKey entry) {
        return entry;
    }
}
