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

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
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

import static org.opensearch.dataprepper.model.annotations.DataPrepperPlugin.DEFAULT_ALTERNATE_NAME;
import static org.opensearch.dataprepper.model.annotations.DataPrepperPlugin.DEFAULT_DEPRECATED_NAME;

/**
 * A {@link PluginProvider} implementation backed by the OSGi service registry.
 * Discovers plugins registered as OSGi services via their {@link DataPrepperPlugin} annotations.
 */
public class OsgiPluginRegistry implements PluginProvider {
    private static final Logger LOG = LoggerFactory.getLogger(OsgiPluginRegistry.class);
    static final String PLUGIN_NAME_PROPERTY = "dataprepper.plugin.name";
    static final String PLUGIN_TYPE_PROPERTY = "dataprepper.plugin.type";

    private final BundleContext bundleContext;
    private volatile Map<String, Map<Class<?>, Class<?>>> nameToSupportedTypeToPluginType;

    public OsgiPluginRegistry(final BundleContext bundleContext) {
        this.bundleContext = bundleContext;
    }

    @Override
    public <T> Optional<Class<? extends T>> findPluginClass(final Class<T> pluginType, final String pluginName) {
        ensureScanned();
        final Map<Class<?>, Class<?>> supportedTypesMap = nameToSupportedTypeToPluginType.get(pluginName);
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
        ensureScanned();
        return nameToSupportedTypeToPluginType.entrySet().stream()
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
     * Scans OSGi service registry for registered plugin classes.
     * Falls back to scanning bundle classes for @DataPrepperPlugin annotations.
     */
    void scanServices() {
        final Map<String, Map<Class<?>, Class<?>>> pluginsMap = new HashMap<>();

        try {
            final ServiceReference<?>[] refs = bundleContext.getAllServiceReferences(null, null);
            if (refs != null) {
                for (final ServiceReference<?> ref : refs) {
                    final String pluginName = (String) ref.getProperty(PLUGIN_NAME_PROPERTY);
                    final String pluginTypeName = (String) ref.getProperty(PLUGIN_TYPE_PROPERTY);
                    if (pluginName != null && pluginTypeName != null) {
                        final String pluginClassName = (String) ref.getProperty("dataprepper.plugin.class");
                        // Manage TCCL during class loading so that any static initializer in
                        // the plugin class that calls ServiceLoader.load(X) resolves against
                        // the bundle classloader rather than the framework's TCCL.
                        try (BundleClassLoaderScope ignored = BundleClassLoaderScope.of(ref.getBundle())) {
                            try {
                                final Class<?> pluginType = ref.getBundle().loadClass(pluginTypeName);
                                if (pluginClassName != null) {
                                    final Class<?> pluginClass = ref.getBundle().loadClass(pluginClassName);
                                    pluginsMap.computeIfAbsent(pluginName, k -> new HashMap<>())
                                            .put(pluginType, pluginClass);
                                } else {
                                    final Object service = bundleContext.getService(ref);
                                    if (service != null) {
                                        pluginsMap.computeIfAbsent(pluginName, k -> new HashMap<>())
                                                .put(pluginType, service.getClass());
                                        bundleContext.ungetService(ref);
                                    }
                                }
                            } catch (final ClassNotFoundException e) {
                                LOG.warn("Plugin class not found for plugin '{}': {}", pluginName, e.getMessage());
                            }
                        }
                    }
                }
            }
        } catch (final InvalidSyntaxException e) {
            LOG.error("Error querying OSGi service registry", e);
        }

        // Also scan bundles for @DataPrepperPlugin annotated classes
        scanBundlesForAnnotations(pluginsMap);

        LOG.debug("OSGi plugin registry found {} plugin names", pluginsMap.size());
        nameToSupportedTypeToPluginType = pluginsMap;
    }

    private void scanBundlesForAnnotations(final Map<String, Map<Class<?>, Class<?>>> pluginsMap) {
        for (final org.osgi.framework.Bundle bundle : bundleContext.getBundles()) {
            if (bundle.getState() != org.osgi.framework.Bundle.ACTIVE) {
                continue;
            }
            final String pluginPackagesHeader = bundle.getHeaders().get("DataPrepper-Plugin-Classes");
            if (pluginPackagesHeader == null) {
                continue;
            }
            try (BundleClassLoaderScope ignored = BundleClassLoaderScope.of(bundle)) {
                for (final String packageName : pluginPackagesHeader.split(",")) {
                    final String trimmed = packageName.trim();
                    if (trimmed.isEmpty()) {
                        continue;
                    }
                    scanPackageForAnnotations(bundle, trimmed, pluginsMap);
                }
            }
        }
    }

    private void scanPackageForAnnotations(final org.osgi.framework.Bundle bundle, final String packageName,
                                           final Map<String, Map<Class<?>, Class<?>>> pluginsMap) {
        final java.util.Enumeration<java.net.URL> entries = bundle.findEntries(
                "/" + packageName.replace('.', '/'), "*.class", true);
        if (entries == null) {
            return;
        }
        while (entries.hasMoreElements()) {
            final java.net.URL classUrl = entries.nextElement();
            final String path = classUrl.getPath();
            final String className = path.replaceFirst("^/", "")
                    .replace('/', '.')
                    .replaceAll("\\.class$", "");
            try {
                final Class<?> clazz = bundle.loadClass(className);
                final DataPrepperPlugin annotation = clazz.getAnnotation(DataPrepperPlugin.class);
                if (annotation != null) {
                    final Class<?> supportedType = annotation.pluginType();
                    pluginsMap.computeIfAbsent(annotation.name(), k -> new HashMap<>())
                            .put(supportedType, clazz);
                    if (!annotation.deprecatedName().equals(DEFAULT_DEPRECATED_NAME)) {
                        pluginsMap.computeIfAbsent(annotation.deprecatedName(), k -> new HashMap<>())
                                .put(supportedType, clazz);
                    }
                    for (final String altName : annotation.alternateNames()) {
                        if (!altName.equals(DEFAULT_ALTERNATE_NAME)) {
                            pluginsMap.computeIfAbsent(altName, k -> new HashMap<>())
                                    .put(supportedType, clazz);
                        }
                    }
                }
            } catch (final ClassNotFoundException | NoClassDefFoundError e) {
                LOG.trace("Could not load class {} from bundle {}: {}",
                        className, bundle.getSymbolicName(), e.getMessage());
            }
        }
    }

    void refresh() {
        synchronized (this) {
            nameToSupportedTypeToPluginType = null;
        }
    }

    // Double-checked locking: volatile field guarantees visibility; synchronized block prevents duplicate scans.
    private void ensureScanned() {
        if (nameToSupportedTypeToPluginType == null) {
            synchronized (this) {
                if (nameToSupportedTypeToPluginType == null) {
                    scanServices();
                }
            }
        }
    }

    /**
     * Returns the number of discovered plugin names.
     *
     * @return the count of unique plugin names
     */
    public int getPluginCount() {
        ensureScanned();
        return nameToSupportedTypeToPluginType.size();
    }
}
