/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.buffer;

import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.PluginFactory;
import com.amazon.dataprepper.plugins.PluginRepository;

/**
 * Old class for creating buffers.
 *
 * @deprecated in 1.2. Use {@link com.amazon.dataprepper.model.plugin.PluginFactory} instead.
 */
@SuppressWarnings({"rawtypes"})
@Deprecated
public class BufferFactory extends PluginFactory {

    public static Buffer newBuffer(final PluginSetting pluginSetting) {
        return (Buffer) newPlugin(pluginSetting, PluginRepository.getBufferClass(pluginSetting.getName()));
    }
}
