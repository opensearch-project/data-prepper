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

package com.amazon.dataprepper.plugins.prepper;

import com.amazon.dataprepper.model.annotations.SingleThread;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.prepper.Prepper;
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

    public static List<Prepper> newPreppers(final PluginSetting pluginSetting) {
        final Class<Prepper> clazz = PluginRepository.getPrepperClass(pluginSetting.getName());
        if (clazz.isAnnotationPresent(SingleThread.class)) {
            final List<Prepper> preppers = new ArrayList<>();
            for (int i = 0; i < pluginSetting.getNumberOfProcessWorkers(); i++) {
                preppers.add((Prepper) newPlugin(pluginSetting, clazz));
            }
            return preppers;
        } else {
            return Collections.singletonList((Prepper) newPlugin(pluginSetting, clazz));
        }
    }
}
