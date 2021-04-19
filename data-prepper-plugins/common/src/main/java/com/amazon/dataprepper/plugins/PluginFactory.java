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

package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import static java.lang.String.format;

public class PluginFactory {
    private static final Logger LOG = LoggerFactory.getLogger(PluginFactory.class);

    public static Object newPlugin(final PluginSetting pluginSetting, final Class<?> clazz) {
        if (clazz == null) {
            LOG.error("Failed to find the plugin with name {}. " +
                    "Please ensure that plugin is annotated with appropriate values", pluginSetting.getName());
            throw new PluginException(format("Failed to find the plugin with name [%s]. " +
                    "Please ensure that plugin is annotated with appropriate values", pluginSetting.getName()));
        }
        try {
            final Constructor<?> constructor = clazz.getConstructor(PluginSetting.class);
            return constructor.newInstance(pluginSetting);
        } catch (NoSuchMethodException e) {
            LOG.error("Data Prepper plugin requires a constructor with {} parameter;" +
                            " Plugin {} with name {} is missing such constructor.", PluginSetting.class.getSimpleName(),
                    clazz.getSimpleName(), pluginSetting.getName(), e);
            throw new PluginException(format("Data Prepper plugin requires a constructor with %s parameter;" +
                            " Plugin %s with name %s is missing such constructor.", PluginSetting.class.getSimpleName(),
                    clazz.getSimpleName(), pluginSetting.getName()), e);
        } catch (IllegalAccessException | InstantiationException | InvocationTargetException e) {
            LOG.error("Encountered exception while instantiating the plugin {}", clazz.getSimpleName(), e);
            throw new PluginException(format("Encountered exception while instantiating the plugin %s",
                    clazz.getSimpleName()), e);
        }
    }
}
