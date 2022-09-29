/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutatestring;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.Processor;

import java.util.Locale;

/**
 * This processor takes in a key and changes its value to an uppercase string. If the value is not a string,
 * no action is performed.
 */
@DataPrepperPlugin(name = "uppercase_string", pluginType = Processor.class, pluginConfigurationType = WithKeysConfig.class)
public class UppercaseStringProcessor extends AbstractStringProcessor<String> {
    @DataPrepperPluginConstructor
    public UppercaseStringProcessor(final PluginMetrics pluginMetrics, final WithKeysConfig config) {
        super(pluginMetrics, config);
    }

    @Override
    protected String getKey(final String entry) {
        return entry;
    }

    @Override
    protected void performKeyAction(final Event recordEvent, final String entry, final String value)
    {
        recordEvent.put(entry, value.toUpperCase(Locale.ROOT));
    }
}
