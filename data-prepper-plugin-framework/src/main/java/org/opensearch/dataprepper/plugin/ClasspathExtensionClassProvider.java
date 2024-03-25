/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.reflections.Reflections;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implements {@link ExtensionClassProvider} using the classpath to detect extensions.
 * This uses the same {@link PluginPackagesSupplier} as {@link ClasspathPluginProvider}.
 */
@Named
public class ClasspathExtensionClassProvider implements ExtensionClassProvider {
    private static final Logger LOG = LoggerFactory.getLogger(ClasspathExtensionClassProvider.class);
    private final Reflections reflections;
    private Set<Class<? extends ExtensionPlugin>> extensionPluginClasses;

    @Inject
    public ClasspathExtensionClassProvider() {
        this(createReflections());
    }

    private static Reflections createReflections() {
        final String[] packages = new PluginPackagesSupplier().get();
        FilterBuilder filterBuilder = new FilterBuilder();
        for (String packageToInclude : packages) {
            filterBuilder = filterBuilder.includePackage(packageToInclude);
        }

        return new Reflections(new ConfigurationBuilder()
                .forPackages(packages)
                .filterInputsBy(filterBuilder));
    }

    /**
     * For testing purposes.
     *
     * @param reflections A {@link Reflections} object.
     */
    ClasspathExtensionClassProvider(final Reflections reflections) {
        this.reflections = reflections;
    }

    @Override
    public Collection<Class<? extends ExtensionPlugin>> loadExtensionPluginClasses() {
        if (extensionPluginClasses == null) {
            extensionPluginClasses = scanForExtensionPlugins();
        }
        return extensionPluginClasses;
    }

    private Set<Class<? extends ExtensionPlugin>> scanForExtensionPlugins() {
        final Set<Class<? extends ExtensionPlugin>> extensionClasses = reflections.getSubTypesOf(ExtensionPlugin.class);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Found {} extension classes.", extensionClasses.size());
            LOG.debug("Extensions classes: {}",
                    extensionClasses.stream().map(Class::getName).collect(Collectors.joining(", ")));
        }

        return extensionClasses;
    }
}
