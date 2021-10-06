package com.amazon.dataprepper.plugin;

import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.plugin.InvalidPluginDefinitionException;
import com.amazon.dataprepper.model.plugin.PluginInvocationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Objects;

import static java.lang.String.format;

class PluginCreator {
    private static final Logger LOG = LoggerFactory.getLogger(PluginCreator.class);

    <T> T newPluginInstance(final Class<T> pluginClass, final PluginSetting pluginSetting) {
        Objects.requireNonNull(pluginClass);

        final String pluginName = pluginSetting.getName();
        final Constructor<?> constructor = getConstructor(pluginClass, pluginName);

        try {
            return (T) constructor.newInstance(pluginSetting);
        } catch (final IllegalAccessException | InstantiationException ex) {
            LOG.error("Encountered exception while instantiating the plugin {}", pluginClass.getSimpleName(), ex);
            throw new InvalidPluginDefinitionException("Unable to access or instantiate the plugin '" + pluginClass.getSimpleName() + ".'", ex);
        } catch (final InvocationTargetException ex) {
            LOG.error("Encountered exception while instantiating the plugin {}", pluginClass.getSimpleName(), ex);
            throw new PluginInvocationException("Exception throw from the plugin'" + pluginClass.getSimpleName() + "'." , ex);
        }
    }

    private <T> Constructor<?> getConstructor(final Class<T> pluginClass, final String pluginName) {
        try {
            return pluginClass.getConstructor(PluginSetting.class);
        } catch (final NoSuchMethodException ex) {
            LOG.error("Data Prepper plugin requires a constructor with {} parameter;" +
                            " Plugin {} with name {} is missing such constructor.", PluginSetting.class.getSimpleName(),
                    pluginClass.getSimpleName(), pluginName, ex);
            throw new InvalidPluginDefinitionException(format("Data Prepper plugin requires a constructor with %s parameter;" +
                            " Plugin %s with name %s is missing such constructor.", PluginSetting.class.getSimpleName(),
                    pluginClass.getSimpleName(), pluginName), ex);
        }
    }
}
