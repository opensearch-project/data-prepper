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

@DataPrepperPlugin(name = "uppercase_string", pluginType = Processor.class, pluginConfigurationType = UppercaseStringProcessorConfig.class)
public class UppercaseStringProcessor extends WithKeysProcessor {
    @DataPrepperPluginConstructor
    public UppercaseStringProcessor(final PluginMetrics pluginMetrics, final UppercaseStringProcessorConfig config) {
        super(pluginMetrics, config);
    }

    @Override
    protected void performKeyAction(Event recordEvent, String key, String value)
    {
        recordEvent.put(key, value.toUpperCase(Locale.ROOT));
    }
}
