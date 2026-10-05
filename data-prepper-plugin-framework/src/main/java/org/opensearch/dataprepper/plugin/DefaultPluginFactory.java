/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.NoPluginFoundException;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.context.annotation.DependsOn;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * The primary implementation of {@link PluginFactory}.
 *
 * @since 1.2
 */
@Named
@DependsOn({"extensionsApplier"})
public class DefaultPluginFactory implements PluginFactory {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultPluginFactory.class);

    private final PluginProviderLoader pluginProviderLoader;
    private final PluginCreator pluginCreator;
    private final PluginConfigurationConverter pluginConfigurationConverter;
    private final PluginBeanFactoryProvider pluginBeanFactoryProvider;
    private final PluginConfigurationObservableFactory pluginConfigurationObservableFactory;
    private final ApplicationContextToTypedSuppliers applicationContextToTypedSuppliers;
    private final List<Consumer<DefinedPlugin<?>>> definedPluginConsumers;
    private final Optional<PluginProviderRegistrar> pluginProviderRegistrar;

    /**
     * @param pluginProviderRegistrar the component which registers plugin providers during its own
     *                                initialization, currently the OSGi framework runner. Declaring it
     *                                here is what forces Spring to complete that registration before
     *                                this factory — and therefore any pipeline — is created; see
     *                                {@link PluginProviderRegistrar}. It is optional because the only
     *                                implementation ships in a module {@code data-prepper-core} depends
     *                                on at runtime only.
     */
    @Inject
    DefaultPluginFactory(
            final PluginProviderLoader pluginProviderLoader,
            @Named("pluginCreator") final PluginCreator pluginCreator,
            final PluginConfigurationConverter pluginConfigurationConverter,
            final PluginBeanFactoryProvider pluginBeanFactoryProvider,
            final PluginConfigurationObservableFactory pluginConfigurationObservableFactory,
            final ApplicationContextToTypedSuppliers applicationContextToTypedSuppliers,
            final List<Consumer<DefinedPlugin<?>>> definedPluginConsumers,
            final Optional<PluginProviderRegistrar> pluginProviderRegistrar) {
        this.applicationContextToTypedSuppliers = applicationContextToTypedSuppliers;
        this.definedPluginConsumers = definedPluginConsumers;
        this.pluginProviderRegistrar = Objects.requireNonNull(pluginProviderRegistrar);
        this.pluginProviderLoader = Objects.requireNonNull(pluginProviderLoader);
        Objects.requireNonNull(pluginConfigurationObservableFactory);
        this.pluginCreator = Objects.requireNonNull(pluginCreator);
        this.pluginConfigurationConverter = Objects.requireNonNull(pluginConfigurationConverter);

        this.pluginBeanFactoryProvider = Objects.requireNonNull(pluginBeanFactoryProvider);
        this.pluginConfigurationObservableFactory = pluginConfigurationObservableFactory;
    }

    @Override
    public <T> T loadPlugin(final Class<T> baseClass, final PluginSetting pluginSetting, final Object... args) {
        final String pluginName = pluginSetting.getName();
        final Class<? extends T> pluginClass = getPluginClass(baseClass, pluginName);

        final ComponentPluginArgumentsContext constructionContext = getConstructionContext(pluginSetting, pluginClass, null);

        return pluginCreator.newPluginInstance(pluginClass, constructionContext, pluginName, args);
    }

    @Override
    public <T> T loadPlugin(final Class<T> baseClass, final PluginSetting pluginSetting, final SinkContext sinkContext) {
        final String pluginName = pluginSetting.getName();
        final Class<? extends T> pluginClass = getPluginClass(baseClass, pluginName);

        final ComponentPluginArgumentsContext constructionContext = getConstructionContext(pluginSetting, pluginClass, sinkContext);

        return pluginCreator.newPluginInstance(pluginClass, constructionContext, pluginName);
    }

    @Override
    public <T> List<T> loadPlugins(
            final Class<T> baseClass, final PluginSetting pluginSetting,
            final Function<Class<? extends T>, Integer> numberOfInstancesFunction) {

        final String pluginName = pluginSetting.getName();
        final Class<? extends T> pluginClass = getPluginClass(baseClass, pluginName);

        final Integer numberOfInstances = numberOfInstancesFunction.apply(pluginClass);

        if (numberOfInstances == null || numberOfInstances < 0)
            throw new IllegalArgumentException("The numberOfInstances must be provided as a non-negative integer.");

        final ComponentPluginArgumentsContext constructionContext = getConstructionContext(pluginSetting, pluginClass, null);

        final List<T> plugins = new ArrayList<>(numberOfInstances);
        for (int i = 0; i < numberOfInstances; i++) {
            plugins.add(pluginCreator.newPluginInstance(pluginClass, constructionContext, pluginName));
        }
        return plugins;
    }

    private <T> ComponentPluginArgumentsContext getConstructionContext(final PluginSetting pluginSetting, final Class<? extends T> pluginClass, final SinkContext sinkContext) {
        final DataPrepperPlugin pluginAnnotation = pluginClass.getAnnotation(DataPrepperPlugin.class);

        final Class<?> pluginConfigurationType = pluginAnnotation.pluginConfigurationType();

        final Object configuration = pluginConfigurationConverter.convert(pluginConfigurationType, pluginSetting, this);
        final PluginConfigObservable pluginConfigObservable = pluginConfigurationObservableFactory
                .createDefaultPluginConfigObservable(pluginConfigurationConverter, pluginConfigurationType, pluginSetting, this);

        Class[] markersToScan = pluginAnnotation.packagesToScan();
        BeanFactory beanFactory = pluginBeanFactoryProvider.createPluginSpecificContext(markersToScan, configuration, pluginSetting);

        return new ComponentPluginArgumentsContext.Builder()
                .withPluginSetting(pluginSetting)
                .withPipelineDescription(pluginSetting)
                .withPluginConfiguration(configuration)
                .withPluginFactory(this)
                .withSinkContext(sinkContext)
                .withBeanFactory(beanFactory)
                .withPluginConfigurationObservable(pluginConfigObservable)
                .withTypeArgumentSuppliers(applicationContextToTypedSuppliers.getArgumentsSuppliers())
                .build();
    }

    private <T> Class<? extends T> getPluginClass(final Class<T> baseClass, final String pluginName) {
        // Re-read the providers on every lookup rather than snapshotting them in the constructor.
        // The PluginProviderRegistrar dependency already forces OSGi registration to complete before
        // this factory is constructed, so this is defense in depth: it keeps a provider registered by
        // any other means after construction visible. See PluginProviderLoader#registerProvider.
        final Collection<PluginProvider> currentProviders = pluginProviderLoader.getPluginProviders();
        if (currentProviders.isEmpty()) {
            throw new RuntimeException(describeMissingPluginProviders());
        }
        final Class<? extends T> pluginClass = currentProviders.stream()
                .map(pluginProvider -> pluginProvider.findPluginClass(baseClass, pluginName))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .findFirst()
                .orElseThrow(() -> new NoPluginFoundException(
                        "Unable to find a plugin named '" + pluginName + "'. Please ensure that plugin is annotated with appropriate values."));

        handleDefinedPlugins(pluginClass, baseClass, pluginName);
        return pluginClass;
    }

    /**
     * Builds the diagnostic for the case where no plugin provider is available. The registrar state is
     * included so that a genuine registration failure is not misreported as a startup ordering problem.
     *
     * @return the message to fail with
     */
    private String describeMissingPluginProviders() {
        final String commonMessage = "Data Prepper requires at least one PluginProvider. " +
                "When running with the default plugin framework, your Data Prepper installation may be " +
                "missing the org.opensearch.dataprepper.plugin.PluginProvider file. ";

        if (!pluginProviderRegistrar.isPresent()) {
            return commonMessage +
                    "No PluginProviderRegistrar is present in this application context, so no OSGi plugin " +
                    "provider was registered. When running with -Ddata-prepper.plugin.framework=osgi, this " +
                    "means the OSGi plugin framework module is not on the classpath.";
        }

        if (pluginProviderRegistrar.get().isPluginProviderRegistrationComplete()) {
            return commonMessage +
                    "The OSGi plugin framework finished starting but registered no plugin provider. " +
                    "Check the startup log for bundle resolution or activation failures.";
        }

        return commonMessage +
                "No OSGi plugin provider has been registered yet, which means the OSGi framework has not " +
                "finished starting.";
    }

    private <T> void handleDefinedPlugins(final Class<? extends T> pluginClass,
                                          final Class<? extends T> pluginTypeClass,
                                          final String pluginName) {
        final DefinedPlugin<? extends T> definedPlugin = new DefinedPlugin<>(pluginClass, pluginTypeClass, pluginName);

        definedPluginConsumers.forEach(definedPluginConsumer -> definedPluginConsumer.accept(definedPlugin));
    }
}
