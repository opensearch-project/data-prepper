/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.annotations.DataPrepperExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.InvalidPluginDefinitionException;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Named
public class ExtensionLoader {
    private final ExtensionPluginConfigurationConverter extensionPluginConfigurationConverter;
    private final ExtensionClassProvider extensionClassProvider;
    private final PluginCreator pluginCreator;

    @Inject
    ExtensionLoader(
            final ExtensionPluginConfigurationConverter extensionPluginConfigurationConverter,
            final ExtensionClassProvider extensionClassProvider,
            final PluginCreator pluginCreator) {
        this.extensionPluginConfigurationConverter = extensionPluginConfigurationConverter;
        this.extensionClassProvider = extensionClassProvider;
        this.pluginCreator = pluginCreator;
    }

    List<? extends ExtensionPlugin> loadExtensions() {
        return extensionClassProvider.loadExtensionPluginClasses()
                .stream()
                .map(extensionClass -> {
                    final PluginArgumentsContext pluginArgumentsContext = getConstructionContext(extensionClass);
                    return pluginCreator.newPluginInstance(extensionClass, pluginArgumentsContext, null, convertClassToName(extensionClass));
                })
                .collect(Collectors.toList());
    }

    private PluginArgumentsContext getConstructionContext(final Class<?> extensionPluginClass) {
        final DataPrepperExtensionPlugin pluginAnnotation = extensionPluginClass.getAnnotation(
                DataPrepperExtensionPlugin.class);
        if (pluginAnnotation == null) {
            return new NoArgumentsArgumentsContext();
        } else {
            final Class<?> pluginConfigurationType = pluginAnnotation.modelType();
            final String rootKey = pluginAnnotation.rootKeyJsonPath();
            final Object configuration = extensionPluginConfigurationConverter.convert(
                    pluginAnnotation.allowInPipelineConfigurations(),
                    pluginConfigurationType, rootKey);
            return new SingleConfigArgumentArgumentsContext(configuration);
        }
    }

    private String convertClassToName(final Class<? extends ExtensionPlugin> extensionClass) {
        final String className = extensionClass.getSimpleName();
        return classNameToPluginName(className);
    }

    static String classNameToPluginName(final String className) {

        final String[] words = className.split("(?=\\p{Upper})");

        return Arrays.stream(words)
                .map(String::toLowerCase)
                .collect(Collectors.joining("_"))
                .replace("$", "");
    }

    protected static class NoArgumentsArgumentsContext implements PluginArgumentsContext {
        @Override
        public Object[] createArguments(final Class<?>[] parameterTypes, Optional<Object> arg) {
            if(parameterTypes.length != 0) {
                throw new InvalidPluginDefinitionException("No arguments are permitted for extensions constructors.");
            }
            return new Object[0];
        }
    }

    protected static class SingleConfigArgumentArgumentsContext implements PluginArgumentsContext {
        private final Object extensionPluginConfiguration;

        SingleConfigArgumentArgumentsContext(final Object extensionPluginConfiguration) {
            this.extensionPluginConfiguration = extensionPluginConfiguration;
        }

        @Override
        public Object[] createArguments(Class<?>[] parameterTypes, Optional<Object> arg) {
            if (parameterTypes.length != 1 && (Objects.nonNull(extensionPluginConfiguration) &&
                    !parameterTypes[0].equals(extensionPluginConfiguration.getClass()))) {
                throw new InvalidPluginDefinitionException(String.format(
                        "Single %s argument is permitted for extensions constructors.",
                        extensionPluginConfiguration.getClass()));
            }
            return new Object[] { extensionPluginConfiguration };
        }
    }
}
