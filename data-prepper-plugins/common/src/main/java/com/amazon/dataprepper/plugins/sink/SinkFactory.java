/*
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  The OpenSearch Contributors require contributions made to
 *  this file be licensed under the Apache-2.0 license or a
 *  compatible open source license.
 *
 *  Modifications Copyright OpenSearch Contributors. See
 *  GitHub history for details.
 */

package com.amazon.dataprepper.plugins.sink;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.plugins.PluginFactory;
import com.amazon.dataprepper.plugins.PluginRepository;

@SuppressWarnings({"rawtypes"})
public class SinkFactory extends PluginFactory {

    public static Sink newSink(final PluginSetting pluginSetting) {
        return (Sink) newPlugin(pluginSetting, PluginRepository.getSinkClass(pluginSetting.getName()));
    }
}
