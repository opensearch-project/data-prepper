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

@DataPrepperPlugin(name = "uppercase_string", pluginType = Processor.class, pluginConfigurationType = StringProcessorConfig.class)
public class UppercaseStringProcessor extends AbstractStringProcessor<String> {
    @DataPrepperPluginConstructor
    public UppercaseStringProcessor(final PluginMetrics pluginMetrics, final StringProcessorConfig<String> config) {
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
