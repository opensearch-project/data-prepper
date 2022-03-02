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

@DataPrepperPlugin(name = "lowercase_string", pluginType = Processor.class, pluginConfigurationType = WithKeysProcessorConfig.class)
public class LowercaseStringProcessor extends WithKeysProcessor {
    @DataPrepperPluginConstructor
    public LowercaseStringProcessor(final PluginMetrics pluginMetrics, final WithKeysProcessorConfig config) {
        super(pluginMetrics, config);
    }

    @Override
    protected void performKeyAction(Event recordEvent, String key, String value)
    {
        recordEvent.put(key, value.toLowerCase(Locale.ROOT));
    }
}
