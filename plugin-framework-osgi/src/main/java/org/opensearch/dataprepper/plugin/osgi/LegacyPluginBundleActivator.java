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
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.ArrayList;
import java.util.Dictionary;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

/**
 * OSGi BundleActivator that bridges legacy Data Prepper plugins into the OSGi
 * service registry. Reads {@code data-prepper.plugins.properties} from the bundle
 * and registers discovered plugin classes as OSGi services.
 */
public class LegacyPluginBundleActivator implements BundleActivator {
    private static final Logger LOG = LoggerFactory.getLogger(LegacyPluginBundleActivator.class);
    private static final String PROPERTIES_PATH = "META-INF/data-prepper.plugins.properties";

    private final List<ServiceRegistration<?>> registrations = new ArrayList<>();

    @Override
    public void start(final BundleContext context) throws Exception {
        LOG.debug("Starting legacy plugin bridge for bundle: {}", context.getBundle().getSymbolicName());

        final String pluginClassesHeader = context.getBundle().getHeaders().get("DataPrepper-Plugin-Classes");
        if (pluginClassesHeader == null || pluginClassesHeader.isEmpty()) {
            LOG.debug("No DataPrepper-Plugin-Classes header found in bundle {}", context.getBundle().getSymbolicName());
            return;
        }

        // Set the TCCL to the bundle's classloader during plugin class loading and
        // registration. This ensures that any ServiceLoader.load(X) calls triggered
        // by static initializers or constructors during class loading resolve against
        // the bundle's own META-INF/services, not the framework's TCCL.
        try (BundleClassLoaderScope ignored = BundleClassLoaderScope.of(context.getBundle())) {
            for (final String className : pluginClassesHeader.split(",")) {
                final String trimmed = className.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                registerPluginPackage(context, trimmed);
            }
        }

        LOG.info("Legacy plugin bridge registered {} services from bundle {}",
                registrations.size(), context.getBundle().getSymbolicName());
    }

    @Override
    public void stop(final BundleContext context) {
        LOG.debug("Stopping legacy plugin bridge for bundle: {}", context.getBundle().getSymbolicName());
        for (final ServiceRegistration<?> registration : registrations) {
            try {
                registration.unregister();
            } catch (final IllegalStateException e) {
                // Already unregistered
            }
        }
        registrations.clear();
    }

    private void registerPluginPackage(final BundleContext context, final String packageName) {
        // The DataPrepper-Plugin-Classes header stores package names from the properties file.
        // We scan the bundle for classes in those packages annotated with @DataPrepperPlugin.
        final Enumeration<URL> entries = context.getBundle().findEntries(
                "/" + packageName.replace('.', '/'), "*.class", true);

        if (entries == null) {
            return;
        }

        while (entries.hasMoreElements()) {
            final URL classUrl = entries.nextElement();
            final String path = classUrl.getPath();
            final String className = path
                    .replaceFirst("^/", "")
                    .replace('/', '.')
                    .replaceAll("\\.class$", "");

            try {
                final Class<?> clazz = context.getBundle().loadClass(className);
                final DataPrepperPlugin annotation = clazz.getAnnotation(DataPrepperPlugin.class);
                if (annotation != null) {
                    registerPluginService(context, clazz, annotation);
                }
            } catch (final ClassNotFoundException | NoClassDefFoundError e) {
                LOG.trace("Could not load class {} from bundle {}: {}",
                        className, context.getBundle().getSymbolicName(), e.getMessage());
            }
        }
    }

    private void registerPluginService(final BundleContext context, final Class<?> pluginClass,
                                       final DataPrepperPlugin annotation) {
        final Dictionary<String, Object> properties = new Hashtable<>();
        properties.put(OsgiPluginRegistry.PLUGIN_NAME_PROPERTY, annotation.name());
        properties.put(OsgiPluginRegistry.PLUGIN_TYPE_PROPERTY, annotation.pluginType().getName());
        properties.put("dataprepper.plugin.class", pluginClass.getName());

        // Register under Class.class — the service object is the plugin Class itself
        // (not an instance of the plugin type), so we cannot register under the plugin
        // type interface (OSGi would reject it). OsgiPluginRegistry finds services via
        // getAllServiceReferences(null, null) and retrieves the plugin type from the
        // PLUGIN_TYPE_PROPERTY service property.
        final ServiceRegistration<?> registration = context.registerService(
                Class.class.getName(), pluginClass, properties);
        registrations.add(registration);

        LOG.debug("Registered OSGi service for plugin '{}' (type: {}, class: {})",
                annotation.name(), annotation.pluginType().getSimpleName(), pluginClass.getName());
    }
}
