package org.opensearch.dataprepper.plugin;

import org.springframework.context.annotation.Bean;

import javax.inject.Named;

import java.util.Arrays;
import java.util.Comparator;

@Named
public class PluginCreatorContext {
    @Bean(name = "extensionPluginCreator")
    public PluginCreator observablePluginCreator() {
        return new PluginCreator();
    }

    @Bean(name = "pluginCreator")
    public PluginCreator pluginCreator(
            final PluginConfigurationObservableRegister pluginConfigurationObservableRegister) {
        return new PluginCreator(pluginConfigurationObservableRegister);
    }

    @Bean(name = "extensionsLoaderComparator")
    public Comparator<ExtensionLoader.ExtensionPluginWithContext> extensionsLoaderComparator() {
        return (extensionOne, extensionTwo) -> {
            // First, compare by configuration status (configured ones first)
            int configCompare = Boolean.compare(extensionTwo.isConfigured(), extensionOne.isConfigured());
            if (configCompare != 0) {
                return configCompare;
            }

            // Get the provided and dependent classes for both extensions
            Class<?>[] extensionOneProvidedClasses = extensionOne.getProvidedClasses();
            Class<?>[] extensionTwoProvidedClasses = extensionTwo.getProvidedClasses();
            Class<?>[] extensionOneDependentClasses = extensionOne.getDependentClasses();
            Class<?>[] extensionTwoDependentClasses = extensionTwo.getDependentClasses();

            // If extensionOne provides any classes that extensionTwo depends on, extensionOne should go first
            if (containsAnyExtensionDependencies(extensionOneProvidedClasses, extensionTwoDependentClasses)) {
                return -1;
            }

            // If extensionTwo provides any classes that extensionOne depends on, extensionTwo should go first
            if (containsAnyExtensionDependencies(extensionTwoProvidedClasses, extensionOneDependentClasses)) {
                return 1;
            }

            return 0;
        };
    }

    private boolean containsAnyExtensionDependencies(final Class<?>[] provided, final Class<?>[] dependencies) {
        return Arrays.stream(dependencies).anyMatch(dep ->
                Arrays.asList(provided).contains(dep));
    }
}
