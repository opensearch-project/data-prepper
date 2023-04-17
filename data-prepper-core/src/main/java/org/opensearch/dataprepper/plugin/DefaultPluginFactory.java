/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.NoPluginFoundException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.event.DefaultEventFactory;
import org.opensearch.dataprepper.acknowledgements.DefaultAcknowledgementSetManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * The primary implementation of {@link PluginFactory}.
 *
 * @since 1.2
 */
@Named
public class DefaultPluginFactory implements PluginFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultPluginFactory.class);

    private final Collection<PluginProvider> pluginProviders;
    private final PluginCreator pluginCreator;
    private final PluginConfigurationConverter pluginConfigurationConverter;
    private final PluginBeanFactoryProvider pluginBeanFactoryProvider;
    private final DefaultEventFactory eventFactory;
    private final DefaultAcknowledgementSetManager acknowledgementSetManager;

    @Inject
    DefaultPluginFactory(
            final PluginProviderLoader pluginProviderLoader,
            final PluginCreator pluginCreator,
            final PluginConfigurationConverter pluginConfigurationConverter,
            final PluginBeanFactoryProvider pluginBeanFactoryProvider,
            final DefaultEventFactory eventFactory,
            final DefaultAcknowledgementSetManager acknowledgementSetManager
    ) {
        Objects.requireNonNull(pluginProviderLoader);
        this.pluginCreator = Objects.requireNonNull(pluginCreator);
        this.pluginConfigurationConverter = Objects.requireNonNull(pluginConfigurationConverter);

        this.pluginProviders = Objects.requireNonNull(pluginProviderLoader.getPluginProviders());
        this.pluginBeanFactoryProvider = Objects.requireNonNull(pluginBeanFactoryProvider);
        this.eventFactory = Objects.requireNonNull(eventFactory);
        this.acknowledgementSetManager = Objects.requireNonNull(acknowledgementSetManager);

        if(pluginProviders.isEmpty()) {
            throw new RuntimeException("Data Prepper requires at least one PluginProvider. " +
                    "Your Data Prepper configuration may be missing the org.opensearch.dataprepper.plugin.PluginProvider file.");
        }
    }

    @Override
    public <T> T loadPlugin(final Class<T> baseClass, final PluginSetting pluginSetting) {
        final String pluginName = pluginSetting.getName();
        final Class<? extends T> pluginClass = getPluginClass(baseClass, pluginName);

        final PluginArgumentsContext constructionContext = getConstructionContext(pluginSetting, pluginClass);

        return pluginCreator.newPluginInstance(pluginClass, constructionContext, pluginName);
    }

    @Override
    public <T> List<T> loadPlugins(
            final Class<T> baseClass, final PluginSetting pluginSetting,
            final Function<Class<? extends T>, Integer> numberOfInstancesFunction) {

        final String pluginName = pluginSetting.getName();
        final Class<? extends T> pluginClass = getPluginClass(baseClass, pluginName);

        final Integer numberOfInstances = numberOfInstancesFunction.apply(pluginClass);

        if(numberOfInstances == null || numberOfInstances < 0)
            throw new IllegalArgumentException("The numberOfInstances must be provided as a non-negative integer.");

        final PluginArgumentsContext constructionContext = getConstructionContext(pluginSetting, pluginClass);

        final List<T> plugins = new ArrayList<>(numberOfInstances);
        for (int i = 0; i < numberOfInstances; i++) {
            plugins.add(pluginCreator.newPluginInstance(pluginClass, constructionContext, pluginName));
        }
        return plugins;
    }

    private <T> PluginArgumentsContext getConstructionContext(final PluginSetting pluginSetting, final Class<? extends T> pluginClass) {
        final DataPrepperPlugin pluginAnnotation = pluginClass.getAnnotation(DataPrepperPlugin.class);

        final Class<?> pluginConfigurationType = pluginAnnotation.pluginConfigurationType();
        final Object configuration = pluginConfigurationConverter.convert(pluginConfigurationType, pluginSetting);

        return new PluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .withPipelineDescription(pluginSetting)
                .withPluginConfiguration(configuration)
                .withPluginFactory(this)
                .withBeanFactory(pluginBeanFactoryProvider.get())
                .withEventFactory(eventFactory)
                .withAcknowledgementSetManager(acknowledgementSetManager)
                .build();
    }

    private <T> Class<? extends T> getPluginClass(final Class<T> baseClass, final String pluginName) {
        final Class<? extends T> pluginClass = pluginProviders.stream()
                .map(pluginProvider -> pluginProvider.findPluginClass(baseClass, pluginName))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst()
                .orElseThrow(() -> new NoPluginFoundException(
                        "Unable to find a plugin named '" + pluginName + "'. Please ensure that plugin is annotated with appropriate values."));

        logDeprecatedPluginsNames(pluginClass, pluginName);
        return pluginClass;
    }

    private <T> void logDeprecatedPluginsNames(final Class<? extends T> pluginClass, final String pluginName) {
        final String deprecatedName = pluginClass.getAnnotation(DataPrepperPlugin.class).deprecatedName();
        final String name = pluginClass.getAnnotation(DataPrepperPlugin.class).name();
        if (deprecatedName.equals(pluginName)) {
            LOG.warn("Plugin name '{}' is deprecated and will be removed in the next major release. Consider using the updated plugin name '{}'.", deprecatedName, name);
        }
    }
}
