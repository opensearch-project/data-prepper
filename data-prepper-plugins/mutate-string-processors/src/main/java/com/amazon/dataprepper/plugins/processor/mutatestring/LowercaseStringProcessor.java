/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutatestring;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.annotations.DataPrepperPluginConstructor;
import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.processor.Processor;
import java.util.Locale;

/**
 * This processor takes in a key and changes its value to a lowercase string. If the value is not a string,
 * no action is performed.
 */
@DataPrepperPlugin(name = "lowercase_string", pluginType = Processor.class, pluginConfigurationType = WithKeysConfig.class)
public class LowercaseStringProcessor extends AbstractStringProcessor<String> {
    @DataPrepperPluginConstructor
    public LowercaseStringProcessor(final PluginMetrics pluginMetrics, final WithKeysConfig config) {
        super(pluginMetrics, config);
    }

    @Override
    protected void performKeyAction(final Event recordEvent, final String key, final String value)
    {
        recordEvent.put(key, value.toLowerCase(Locale.ROOT));
    }

    @Override
    protected String getKey(final String entry) {
        return entry;
    }
}
