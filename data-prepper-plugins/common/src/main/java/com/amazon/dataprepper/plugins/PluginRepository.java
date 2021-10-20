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

import com.amazon.dataprepper.model.PluginType;
import com.amazon.dataprepper.model.annotations.DataPrepperPlugin;
import com.amazon.dataprepper.model.buffer.Buffer;
import com.amazon.dataprepper.model.prepper.Prepper;
import com.amazon.dataprepper.model.sink.Sink;
import com.amazon.dataprepper.model.source.Source;
import org.reflections.Reflections;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Plugin repository for Data Prepper - provides methods to discover new plugins
 * and also the default provided plugins.
 * <p>
 * TODO Add capability to discover custom new plugins
 *
 * @deprecated in 1.2. Use {@link com.amazon.dataprepper.model.plugin.PluginFactory}
 */
@SuppressWarnings({"rawtypes", "unchecked"})
@Deprecated
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
            final PluginType pluginType = extractPluginType(dataPrepperPluginAnnotation);
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

    private static PluginType extractPluginType(final DataPrepperPlugin dataPrepperPluginAnnotation) {
        PluginType pluginType = dataPrepperPluginAnnotation.type();
        if(pluginType == PluginType.NONE) {
            final Class<?> pluginClassType = dataPrepperPluginAnnotation.pluginType();
            if(Objects.equals(pluginClassType, Source.class)) {
                pluginType = PluginType.SOURCE;
            }
            else if(Objects.equals(pluginClassType, Buffer.class)) {
                pluginType = PluginType.BUFFER;
            }
            else if(Objects.equals(pluginClassType, Prepper.class)) {
                pluginType = PluginType.PREPPER;
            }
            else if(Objects.equals(pluginClassType, Sink.class)) {
                pluginType = PluginType.SINK;
            }
        }
        return pluginType;
    }

    public static Class<Source> getSourceClass(final String name) {
        return SOURCES.get(name);
    }

    public static Class<Buffer> getBufferClass(final String name) {
        return BUFFERS.get(name);
    }

    public static Class<Prepper> getPrepperClass(final String name) {
        return PREPPERS.get(name);
    }

    public static Class<Sink> getSinkClass(final String name) {
        return SINKS.get(name);
    }

}
