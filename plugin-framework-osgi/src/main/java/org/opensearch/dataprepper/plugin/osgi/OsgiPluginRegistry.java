/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 */

package org.opensearch.dataprepper.plugin.osgi;

import org.opensearch.dataprepper.plugin.PluginProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.InvalidSyntaxException;
import org.osgi.framework.ServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * A {@link PluginProvider} implementation backed by the OSGi service registry.
 * <p>
 * The service registry is the single source of truth for OSGi plugin discovery. Each bundle's
 * {@link LegacyPluginBundleActivator} loads the bundle's {@code @DataPrepperPlugin} annotated
 * classes once at bundle start and publishes one service per plugin class, carrying every name the
 * plugin answers to (its name, deprecated name, and alternate names). This registry only reads
 * those service properties, so no plugin class is loaded a second time here and no unrelated
 * service is touched.
 */
public class OsgiPluginRegistry implements PluginProvider {
    private static final Logger LOG = LoggerFactory.getLogger(OsgiPluginRegistry.class);
    static final String PLUGIN_NAME_PROPERTY = "dataprepper.plugin.name";
    static final String PLUGIN_TYPE_PROPERTY = "dataprepper.plugin.type";
    static final String PLUGIN_CLASS_PROPERTY = "dataprepper.plugin.class";

    /**
     * Restricts the service lookup to Data Prepper plugin services. Without this filter the scan
     * would walk every service in the framework and, for services with no plugin class property,
     * force-activate them through {@code getService()}.
     */
    private static final String PLUGIN_SERVICE_FILTER = "(" + PLUGIN_NAME_PROPERTY + "=*)";

    private final BundleContext bundleContext;
    private volatile Map<String, Map<Class<?>, Class<?>>> nameToSupportedTypeToPluginType;

    public OsgiPluginRegistry(final BundleContext bundleContext) {
        this.bundleContext = bundleContext;
    }

    @Override
    public <T> Optional<Class<? extends T>> findPluginClass(final Class<T> pluginType, final String pluginName) {
        final Map<Class<?>, Class<?>> supportedTypesMap = scannedPlugins().get(pluginName);
        if (supportedTypesMap == null) {
            return Optional.empty();
        }
        final Class<?> candidateClass = supportedTypesMap.get(pluginType);
        if (candidateClass == null) {
            return Optional.empty();
        }
        if (!pluginType.isAssignableFrom(candidateClass)) {
            LOG.warn("Plugin class {} is not assignable to type {} for plugin '{}'; skipping",
                    candidateClass.getName(), pluginType.getName(), pluginName);
            return Optional.empty();
        }
        @SuppressWarnings("unchecked")
        final Class<? extends T> result = (Class<? extends T>) candidateClass;
        return Optional.of(result);
    }

    @Override
    public <T> Collection<Class<? extends T>> findPluginClasses(final Class<T> pluginType) {
        return scannedPlugins().entrySet().stream()
                .flatMap(outerEntry ->
                        outerEntry.getValue().entrySet().stream()
                                .filter(entry -> pluginType.equals(entry.getKey()))
                                .filter(entry -> {
                                    if (!pluginType.isAssignableFrom(entry.getValue())) {
                                        LOG.warn("Plugin class {} is not assignable to type {} for plugin '{}'; skipping",
                                                entry.getValue().getName(), pluginType.getName(), outerEntry.getKey());
                                        return false;
                                    }
                                    return true;
                                })
                                .map(entry -> {
                                    @SuppressWarnings("unchecked")
                                    final Class<? extends T> result = (Class<? extends T>) entry.getValue();
                                    return result;
                                }))
                .collect(Collectors.toSet());
    }

    /**
     * Scans the OSGi service registry for plugin services published by
     * {@link LegacyPluginBundleActivator}.
     *
     * @return the newly scanned plugin map, which is also published to readers
     */
    Map<String, Map<Class<?>, Class<?>>> scanServices() {
        final Map<String, Map<Class<?>, Class<?>>> pluginsMap = new HashMap<>();

        try {
            final ServiceReference<?>[] refs = bundleContext.getAllServiceReferences(
                    Class.class.getName(), PLUGIN_SERVICE_FILTER);
            if (refs != null) {
                for (final ServiceReference<?> ref : refs) {
                    addPluginService(ref, pluginsMap);
                }
            }
        } catch (final InvalidSyntaxException e) {
            LOG.error("Error querying OSGi service registry with filter {}", PLUGIN_SERVICE_FILTER, e);
        }

        LOG.debug("OSGi plugin registry found {} plugin names", pluginsMap.size());
        nameToSupportedTypeToPluginType = pluginsMap;
        return pluginsMap;
    }

    private void addPluginService(final ServiceReference<?> ref,
                                 final Map<String, Map<Class<?>, Class<?>>> pluginsMap) {
        final String[] pluginNames = toPluginNames(ref.getProperty(PLUGIN_NAME_PROPERTY));
        final String pluginTypeName = (String) ref.getProperty(PLUGIN_TYPE_PROPERTY);
        final String pluginClassName = (String) ref.getProperty(PLUGIN_CLASS_PROPERTY);
        if (pluginNames.length == 0 || pluginTypeName == null || pluginClassName == null) {
            LOG.warn("Ignoring plugin service from bundle {} because it is missing the {}, {} or {} property",
                    ref.getBundle().getSymbolicName(),
                    PLUGIN_NAME_PROPERTY, PLUGIN_TYPE_PROPERTY, PLUGIN_CLASS_PROPERTY);
            return;
        }

        // Manage TCCL during class loading so that any static initializer in the plugin class that
        // calls ServiceLoader.load(X) resolves against the bundle classloader rather than the
        // framework's TCCL.
        try (BundleClassLoaderScope ignored = BundleClassLoaderScope.of(ref.getBundle())) {
            final Class<?> pluginType = ref.getBundle().loadClass(pluginTypeName);
            final Class<?> pluginClass = ref.getBundle().loadClass(pluginClassName);
            for (final String pluginName : pluginNames) {
                pluginsMap.computeIfAbsent(pluginName, name -> new HashMap<>()).put(pluginType, pluginClass);
            }
        } catch (final ClassNotFoundException | NoClassDefFoundError e) {
            LOG.warn("Plugin class not found for plugin '{}': {}", pluginNames[0], e.getMessage());
        }
    }

    /**
     * Reads the plugin-name service property, which carries every name a plugin answers to. OSGi
     * permits a multi-valued property to be published as an array, so accept both shapes.
     */
    private static String[] toPluginNames(final Object nameProperty) {
        if (nameProperty instanceof String[]) {
            return (String[]) nameProperty;
        }
        if (nameProperty instanceof String) {
            return new String[] {(String) nameProperty};
        }
        return new String[0];
    }

    void refresh() {
        synchronized (this) {
            nameToSupportedTypeToPluginType = null;
        }
    }

    /**
     * Returns the current plugin map, scanning first if necessary.
     * <p>
     * Double-checked locking: the volatile field guarantees visibility and the synchronized block prevents
     * duplicate scans. The field is read into a local exactly once so that a concurrent {@link #refresh()},
     * which publishes {@code null}, cannot turn a reader's second read into a {@link NullPointerException}.
     * Such a reader simply completes against the previous snapshot and the next reader triggers the rescan.
     */
    private Map<String, Map<Class<?>, Class<?>>> scannedPlugins() {
        final Map<String, Map<Class<?>, Class<?>>> plugins = nameToSupportedTypeToPluginType;
        if (plugins != null) {
            return plugins;
        }
        synchronized (this) {
            final Map<String, Map<Class<?>, Class<?>>> rescanned = nameToSupportedTypeToPluginType;
            return rescanned != null ? rescanned : scanServices();
        }
    }

    /**
     * Returns the number of discovered plugin names.
     *
     * @return the count of unique plugin names
     */
    public int getPluginCount() {
        return scannedPlugins().size();
    }
}
