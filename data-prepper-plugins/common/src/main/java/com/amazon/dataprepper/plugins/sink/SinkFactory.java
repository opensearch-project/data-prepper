/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.sink;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.plugins.PluginFactory;
import com.amazon.dataprepper.plugins.PluginRepository;

/**
 * Old class for creating sinks.
 *
 * @deprecated in 1.2. Use {@link com.amazon.dataprepper.model.plugin.PluginFactory} instead.
 */
@SuppressWarnings({"rawtypes"})
@Deprecated
public class SinkFactory extends PluginFactory {

    public static Sink newSink(final PluginSetting pluginSetting) {
        return (Sink) newPlugin(pluginSetting, PluginRepository.getSinkClass(pluginSetting.getName()));
    }
}
