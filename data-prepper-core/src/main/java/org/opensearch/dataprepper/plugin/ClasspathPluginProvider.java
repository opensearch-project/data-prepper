/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.dataprepper.model.annotations.DataPrepperPlugin.DEFAULT_DEPRECATED_NAME;

/**
 * The implementation of {@link PluginProvider} which loads plugins from the
 * current Java classpath.
 *
 * @since 1.2
 */
public class ClasspathPluginProvider implements PluginProvider {

    private final Reflections reflections;
    private Map<String, Map<Class<?>, Class<?>>> nameToSupportedTypeToPluginType;

    public ClasspathPluginProvider() {
        this(new Reflections(new ConfigurationBuilder()
                .forPackages(new PluginPackagesSupplier().get()))
        );
    }

    /**
     * For testing only
     */
    ClasspathPluginProvider(final Reflections reflections) {
        this.reflections = reflections;
    }

    @Override
    public <T> Optional<Class<? extends T>> findPluginClass(final Class<T> pluginType, final String pluginName) {
        if (nameToSupportedTypeToPluginType == null) {
            nameToSupportedTypeToPluginType = scanForPlugins();
        }

        final Map<Class<?>, Class<?>> supportedTypesMap = nameToSupportedTypeToPluginType.get(pluginName);

        if(supportedTypesMap == null) {
            return Optional.empty();
        }
        return Optional.ofNullable((Class<? extends T>) supportedTypesMap.get(pluginType));
    }

    private Map<String, Map<Class<?>, Class<?>>> scanForPlugins() {
        final Set<Class<?>> dataPrepperPluginClasses =
                reflections.getTypesAnnotatedWith(DataPrepperPlugin.class);

        final Map<String, Map<Class<?>, Class<?>>> pluginsMap = new HashMap<>(dataPrepperPluginClasses.size());
        for (final Class<?> concretePluginClass : dataPrepperPluginClasses) {
            final DataPrepperPlugin dataPrepperPluginAnnotation = concretePluginClass.getAnnotation(DataPrepperPlugin.class);
            final String pluginName = dataPrepperPluginAnnotation.name();
            final Class<?> supportedType = dataPrepperPluginAnnotation.pluginType();

            final Map<Class<?>, Class<?>> supportTypeToPluginTypeMap =
                    pluginsMap.computeIfAbsent(pluginName, k -> new HashMap<>());
            supportTypeToPluginTypeMap.put(supportedType, concretePluginClass);

            addOptionalDeprecatedPluginName(pluginsMap, concretePluginClass);
        }

        return pluginsMap;
    }

    private void addOptionalDeprecatedPluginName(
            final Map<String, Map<Class<?>, Class<?>>> pluginsMap,
            final Class<?> concretePluginClass) {
        final DataPrepperPlugin dataPrepperPluginAnnotation = concretePluginClass.getAnnotation(DataPrepperPlugin.class);
        final String deprecatedPluginName = dataPrepperPluginAnnotation.deprecatedName();
        final Class<?> supportedType = dataPrepperPluginAnnotation.pluginType();

        if (!deprecatedPluginName.equals(DEFAULT_DEPRECATED_NAME)) {
            final Map<Class<?>, Class<?>> supportTypeToPluginTypeMap =
                    pluginsMap.computeIfAbsent(deprecatedPluginName, k -> new HashMap<>());
            supportTypeToPluginTypeMap.put(supportedType, concretePluginClass);
        }
    }
}
