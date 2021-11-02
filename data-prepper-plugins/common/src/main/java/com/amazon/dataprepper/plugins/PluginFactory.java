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
import com.amazon.dataprepper.model.plugin.InvalidPluginDefinitionException;
import com.amazon.dataprepper.model.plugin.PluginInvocationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.function.BiFunction;

import static java.lang.String.format;

/**
 * Old class for creating plugins.
 *
 * @deprecated in 1.2. Use {@link com.amazon.dataprepper.model.plugin.PluginFactory} instead.
 */
@Deprecated
public class PluginFactory {
    private static final Logger LOG = LoggerFactory.getLogger(PluginFactory.class);
    private static BiFunction<PluginSetting, Class<?>, Object> newPluginFunction;

    /**
     * Please do not call this method. Only the DefaultPluginFactory should call it.
     * <p>
     * This exists so that this class can still exhibit the correct behavior when creating new plugin
     * classes. This whole class is going to be deleted in the next major version, as will this method.
     */
    public static void dangerousMethod_setPluginFunction(final BiFunction<PluginSetting, Class<?>, Object> newPluginFunction) {
        PluginFactory.newPluginFunction = newPluginFunction;
    }

    public static Object newPlugin(final PluginSetting pluginSetting, final Class<?> clazz) {
        if(newPluginFunction != null) {
            try {
                return newPluginFunction.apply(pluginSetting, clazz);
            } catch (final InvalidPluginDefinitionException | PluginInvocationException ex) {
                throw new PluginInvocationException("Failed to create instance of new plugin.", ex);
            }
        }
        return defaultFunction(pluginSetting, clazz);
    }

    private static Object defaultFunction(final PluginSetting pluginSetting, final Class<?> clazz) {
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
