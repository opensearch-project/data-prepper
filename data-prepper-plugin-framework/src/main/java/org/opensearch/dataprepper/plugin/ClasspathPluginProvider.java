/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.opensearch.dataprepper.model.annotations.DataPrepperPlugin.DEFAULT_ALTERNATE_NAME;
import static org.opensearch.dataprepper.model.annotations.DataPrepperPlugin.DEFAULT_DEPRECATED_NAME;

/**
 * The implementation of {@link PluginProvider} which loads plugins from the
 * current Java classpath.
 *
 * @since 1.2
 */
public class ClasspathPluginProvider implements PluginProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ClasspathPluginProvider.class);
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

    @Override
    public <T> Set<Class<? extends T>> findPluginClasses(Class<T> pluginType) {
        if (nameToSupportedTypeToPluginType == null) {
            nameToSupportedTypeToPluginType = scanForPlugins();
        }

        return nameToSupportedTypeToPluginType.values().stream()
                .flatMap(supportedTypeToPluginType ->
                        supportedTypeToPluginType.entrySet().stream()
                                .filter(entry -> pluginType.equals(entry.getKey()))
                                .flatMap(entry -> Stream.of((Class<? extends T>) entry.getValue())))
                .collect(Collectors.toSet());
    }

    private Map<String, Map<Class<?>, Class<?>>> scanForPlugins() {
        final Set<Class<?>> dataPrepperPluginClasses =
                reflections.getTypesAnnotatedWith(DataPrepperPlugin.class);

        if(LOG.isDebugEnabled()) {
            LOG.debug("Found {} plugin classes.", dataPrepperPluginClasses.size());
            LOG.debug("Plugin classes: {}",
                    dataPrepperPluginClasses.stream().map(Class::getName).collect(Collectors.joining(", ")));
        }

        final Map<String, Map<Class<?>, Class<?>>> pluginsMap = new HashMap<>(dataPrepperPluginClasses.size());
        for (final Class<?> concretePluginClass : dataPrepperPluginClasses) {
            final DataPrepperPlugin annotation = concretePluginClass.getAnnotation(DataPrepperPlugin.class);
            if (annotation == null) {
                LOG.warn("Class {} was found by Reflections but does not have @DataPrepperPlugin annotation", concretePluginClass.getName());
                continue;
            }
            // plugin name
            addPossiblePluginName(pluginsMap, concretePluginClass, DataPrepperPlugin::name, name -> true);
            // deprecated plugin name
            addPossiblePluginName(pluginsMap, concretePluginClass, DataPrepperPlugin::deprecatedName,
                    deprecatedPluginName -> !deprecatedPluginName.equals(DEFAULT_DEPRECATED_NAME));
            // alternate plugin names
            for (final String alternateName: annotation.alternateNames()) {
                addPossiblePluginName(pluginsMap, concretePluginClass, DataPrepperPlugin -> alternateName,
                        alternatePluginName -> !alternatePluginName.equals(DEFAULT_ALTERNATE_NAME));
            }
        }

        return pluginsMap;
    }

    private void addPossiblePluginName(
            final Map<String, Map<Class<?>, Class<?>>> pluginsMap,
            final Class<?> concretePluginClass,
            final Function<DataPrepperPlugin, String> possiblePluginNameFunction,
            final Predicate<String> possiblePluginNamePredicate
            ) {
        final DataPrepperPlugin dataPrepperPluginAnnotation = concretePluginClass.getAnnotation(DataPrepperPlugin.class);
        if (dataPrepperPluginAnnotation == null) {
            LOG.warn("Class {} was found by Reflections but does not have @DataPrepperPlugin annotation", concretePluginClass.getName());
            return;
        }
        final String possiblePluginName = possiblePluginNameFunction.apply(dataPrepperPluginAnnotation);
        final Class<?> supportedType = dataPrepperPluginAnnotation.pluginType();

        if (possiblePluginNamePredicate.test(possiblePluginName)) {
            final Map<Class<?>, Class<?>> supportTypeToPluginTypeMap =
                    pluginsMap.computeIfAbsent(possiblePluginName, k -> new HashMap<>());
            supportTypeToPluginTypeMap.put(supportedType, concretePluginClass);
        }
    }
}
