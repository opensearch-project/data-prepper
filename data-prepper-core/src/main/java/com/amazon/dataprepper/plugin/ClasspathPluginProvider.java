package com.amazon.dataprepper.plugin;

import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The implementation of {@link PluginProvider} which loads plugins from the
 * current Java classpath.
 *
 * @since 1.2
 */
public class ClasspathPluginProvider implements PluginProvider {

    private static final String DEFAULT_PLUGINS_CLASSPATH = "com.amazon.dataprepper.plugins";
    private final Reflections reflections;
    private Map<String, Map<Class<?>, Class<?>>> nameToSupportedTypeToPluginType;

    public ClasspathPluginProvider() {
        this(new Reflections(DEFAULT_PLUGINS_CLASSPATH));
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
            final DataPrepperPlugin dataPrepperPluginAnnotation = concretePluginClass
                    .getAnnotation(DataPrepperPlugin.class);
            final String pluginName = dataPrepperPluginAnnotation.name();
            Class<?> supportedType = dataPrepperPluginAnnotation.pluginType();

            if(supportedType.equals(Void.class)) {
                supportedType = dataPrepperPluginAnnotation.type().pluginClass();
            }

            final Map<Class<?>, Class<?>> supportTypeToPluginTypeMap =
                    pluginsMap.computeIfAbsent(pluginName, k -> new HashMap<>());
            supportTypeToPluginTypeMap.put(supportedType, concretePluginClass);
        }

        return pluginsMap;
    }
}
