package com.amazon.dataprepper.plugins;

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.model.source.Source;
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Plugin repository for Data Prepper - provides methods to discover new plugins
 * and also the default provided plugins.
 * <p>
 * TODO Add capability to discover custom new plugins
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public final class  PluginRepository {
    private static final String DEFAULT_PLUGINS_CLASSPATH = "com.amazon.dataprepper.plugins";
    private static final Map<String, Class<Source>> SOURCES = new HashMap<>();
    private static final Map<String, Class<Buffer>> BUFFERS = new HashMap<>();
    private static final Map<String, Class<Prepper>> PREPPERS = new HashMap<>();
    private static final Map<String, Class<Sink>> SINKS = new HashMap<>();

    static {
        findPlugins();
    }

    private static void findPlugins() {
        final Reflections reflections = new Reflections(DEFAULT_PLUGINS_CLASSPATH);
        final Set<Class<?>> annotatedClasses = reflections.getTypesAnnotatedWith(DataPrepperPlugin.class);
        for (final Class<?> annotatedClass : annotatedClasses) {
            final DataPrepperPlugin dataPrepperPluginAnnotation = annotatedClass
                    .getAnnotation(DataPrepperPlugin.class);
            final String pluginName = dataPrepperPluginAnnotation.name();
            final PluginType pluginType = dataPrepperPluginAnnotation.type();
            switch (pluginType) {
                case SOURCE:
                    SOURCES.put(pluginName, (Class<Source>) annotatedClass);
                    break;
                case BUFFER:
                    BUFFERS.put(pluginName, (Class<Buffer>) annotatedClass);
                    break;
                case PREPPER:
                    PREPPERS.put(pluginName, (Class<Prepper>) annotatedClass);
                    break;
                case SINK:
                    SINKS.put(pluginName, (Class<Sink>) annotatedClass);
                    break;
            }
        }
    }

    public static Class<?> getPluginClass(final String name, final PluginType pluginType) {
        switch (pluginType) {
            case SOURCE:
                return getSourceClass(name);
            case BUFFER:
                return getBufferClass(name);
            case PREPPER:
                return getProcessorClass(name);
            case SINK:
                return getSinkClass(name);
        }
        throw new PluginException("Unrecognized plugin type: " + pluginType);
    }

    public static Class<Source> getSourceClass(final String name) {
        return SOURCES.get(name);
    }

    public static Class<Buffer> getBufferClass(final String name) {
        return BUFFERS.get(name);
    }

    public static Class<Prepper> getProcessorClass(final String name) {
        return PREPPERS.get(name);
    }

    public static Class<Sink> getSinkClass(final String name) {
        return SINKS.get(name);
    }

}
