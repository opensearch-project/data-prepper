/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugin;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PipelineDescription;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.plugin.InvalidPluginDefinitionException;
import com.amazon.dataprepper.model.plugin.PluginFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanFactory;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * An internal class which represents all the data which can be provided
 * when constructing a new plugin.
 */
class PluginArgumentsContext {
    private static final String UNABLE_TO_CREATE_PLUGIN_PARAMETER = "Unable to create an argument for required plugin parameter type: ";
    private final Map<Class<?>, Supplier<Object>> typedArgumentsSuppliers;

    @Nullable
    private final BeanFactory beanFactory;

    private PluginArgumentsContext(final Builder builder) {
        Objects.requireNonNull(builder.pluginSetting,
                "PluginArgumentsContext received a null Builder object. This is likely an error in the plugin framework.");

        beanFactory = builder.beanFactory;

        typedArgumentsSuppliers = new HashMap<>();

        typedArgumentsSuppliers.put(builder.pluginSetting.getClass(), () -> builder.pluginSetting);

        if(builder.pluginConfiguration != null) {
            typedArgumentsSuppliers.put(builder.pluginConfiguration.getClass(), () -> builder.pluginConfiguration);
        }

        typedArgumentsSuppliers.put(PluginMetrics.class, () -> PluginMetrics.fromPluginSetting(builder.pluginSetting));

        if (builder.pipelineDescription != null) {
            typedArgumentsSuppliers.put(PipelineDescription.class, () -> builder.pipelineDescription);
        }

        if(builder.pluginFactory != null)
            typedArgumentsSuppliers.put(PluginFactory.class, () -> builder.pluginFactory);
    }

    Object[] createArguments(final Class<?>[] parameterTypes) {
        return Arrays.stream(parameterTypes)
                .map(this::getRequiredArgumentSupplier)
                .map(Supplier::get)
                .toArray();
    }

    private Supplier<Object> getRequiredArgumentSupplier(final Class<?> parameterType) {
        if(typedArgumentsSuppliers.containsKey(parameterType)) {
            return typedArgumentsSuppliers.get(parameterType);
        }
        else {
            return createBeanSupplier(parameterType);
        }
    }

    private Supplier<Object> createBeanSupplier(final Class<?> parameterType) {
        if (beanFactory == null) {
            throw new InvalidPluginDefinitionException(UNABLE_TO_CREATE_PLUGIN_PARAMETER + parameterType);
        }
        else {
            try {
                final Object bean = beanFactory.getBean(parameterType);
                return () -> bean;
            } catch (final BeansException e) {
                throw new InvalidPluginDefinitionException(UNABLE_TO_CREATE_PLUGIN_PARAMETER + parameterType, e);
            }
        }
    }

    static class Builder {
        private Object pluginConfiguration;
        private PluginSetting pluginSetting;
        private PluginFactory pluginFactory;
        private PipelineDescription pipelineDescription;
        private BeanFactory beanFactory;

        Builder withPluginConfiguration(final Object pluginConfiguration) {
            this.pluginConfiguration = pluginConfiguration;
            return this;
        }

        Builder withPluginSetting(final PluginSetting pluginSetting) {
            this.pluginSetting = pluginSetting;
            return this;
        }

        Builder withPluginFactory(final PluginFactory pluginFactory) {
            this.pluginFactory = pluginFactory;
            return this;
        }

        Builder withPipelineDescription(final PipelineDescription pipelineDescription) {
            this.pipelineDescription = pipelineDescription;
            return this;
        }

        Builder withBeanFactory(final BeanFactory beanFactory) {
            this.beanFactory = beanFactory;
            return this;
        }

        PluginArgumentsContext build() {
            return new PluginArgumentsContext(this);
        }
    }
}
