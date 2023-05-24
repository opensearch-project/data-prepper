/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.plugin.ExtensionPlugin;
import org.opensearch.dataprepper.model.plugin.InvalidPluginDefinitionException;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Named
public class ExtensionLoader {
    private final ExtensionClassProvider extensionClassProvider;
    private final PluginCreator pluginCreator;

    @Inject
    ExtensionLoader(
            final ExtensionClassProvider extensionClassProvider,
            final PluginCreator pluginCreator) {
        this.extensionClassProvider = extensionClassProvider;
        this.pluginCreator = pluginCreator;
    }

    List<? extends ExtensionPlugin> loadExtensions() {
        final PluginArgumentsContext pluginArgumentsContext = new NoArgumentsArgumentsContext();

        return extensionClassProvider.loadExtensionPluginClasses()
                .stream()
                .map(extensionClass -> pluginCreator.newPluginInstance(extensionClass, pluginArgumentsContext, convertClassToName(extensionClass)))
                .collect(Collectors.toList());
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

    private static class NoArgumentsArgumentsContext implements PluginArgumentsContext {
        @Override
        public Object[] createArguments(final Class<?>[] parameterTypes) {
            if(parameterTypes.length != 0) {
                throw new InvalidPluginDefinitionException("No arguments are permitted for extensions constructors.");
            }
            return new Object[0];
        }
    }
}
