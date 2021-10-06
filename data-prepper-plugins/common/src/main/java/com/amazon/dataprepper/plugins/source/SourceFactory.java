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

package com.amazon.dataprepper.plugins.source;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.plugins.PluginFactory;
import com.amazon.dataprepper.plugins.PluginRepository;
import com.amazon.dataprepper.model.source.Source;

/**
 * Old class for creating sources.
 *
 * @deprecated in 1.2. Use {@link com.amazon.dataprepper.model.plugin.PluginFactory} instead.
 */
@SuppressWarnings({"rawtypes"})
@Deprecated
public class SourceFactory extends PluginFactory {

    public static Source newSource(final PluginSetting pluginSetting) {
        return (Source) newPlugin(pluginSetting, PluginRepository.getSourceClass(pluginSetting.getName()));
    }
}