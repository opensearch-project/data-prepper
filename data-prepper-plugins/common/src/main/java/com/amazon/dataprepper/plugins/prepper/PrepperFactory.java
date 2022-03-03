/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.annotations.SingleThread;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.processor.Processor;
import com.amazon.dataprepper.plugins.PluginFactory;
import com.amazon.dataprepper.plugins.PluginRepository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Old class for creating Preppers.
 *
 * @deprecated in 1.2. Use {@link com.amazon.dataprepper.model.plugin.PluginFactory} instead.
 */
@SuppressWarnings({"rawtypes"})
@Deprecated
public class PrepperFactory extends PluginFactory {

    public static List<Processor> newPreppers(final PluginSetting pluginSetting) {
        final Class<Processor> clazz = PluginRepository.getPrepperClass(pluginSetting.getName());
        if (clazz.isAnnotationPresent(SingleThread.class)) {
            final List<Processor> preppers = new ArrayList<>();
            for (int i = 0; i < pluginSetting.getNumberOfProcessWorkers(); i++) {
                preppers.add((Processor) newPlugin(pluginSetting, clazz));
            }
            return preppers;
        } else {
            return Collections.singletonList((Processor) newPlugin(pluginSetting, clazz));
        }
    }
}
