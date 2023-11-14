/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import org.opensearch.dataprepper.model.breaker.CircuitBreaker;
import org.opensearch.dataprepper.model.plugin.PluginConfigObservable;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.InvalidPluginDefinitionException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.event.EventFactory;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
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
class ComponentPluginArgumentsContext implements PluginArgumentsContext {
    private static final String UNABLE_TO_CREATE_PLUGIN_PARAMETER = "Unable to create an argument for required plugin parameter type: ";
    private final Map<Class<?>, Supplier<Object>> typedArgumentsSuppliers;

    @Nullable
    private final BeanFactory beanFactory;

    private ComponentPluginArgumentsContext(final Builder builder) {
        Objects.requireNonNull(builder.pluginSetting,
                "PluginArgumentsContext received a null Builder object. This is likely an error in the plugin framework.");

        beanFactory = builder.beanFactory;

        typedArgumentsSuppliers = new HashMap<>();

        typedArgumentsSuppliers.put(PluginSetting.class, () -> builder.pluginSetting);

        if(builder.pluginConfiguration != null) {
            typedArgumentsSuppliers.put(builder.pluginConfiguration.getClass(), () -> builder.pluginConfiguration);
        }

        typedArgumentsSuppliers.put(PluginMetrics.class, () -> PluginMetrics.fromPluginSetting(builder.pluginSetting));

        if (builder.pipelineDescription != null) {
            typedArgumentsSuppliers.put(PipelineDescription.class, () -> builder.pipelineDescription);
        }

        if (builder.pluginFactory != null) {
            typedArgumentsSuppliers.put(PluginFactory.class, () -> builder.pluginFactory);
        }

        if (builder.eventFactory != null) {
            typedArgumentsSuppliers.put(EventFactory.class, () -> builder.eventFactory);
        }

        if (builder.acknowledgementSetManager != null) {
            typedArgumentsSuppliers.put(AcknowledgementSetManager.class, () -> builder.acknowledgementSetManager);
        }

        if (builder.pluginConfigObservable != null) {
            typedArgumentsSuppliers.put(PluginConfigObservable.class, () -> builder.pluginConfigObservable);
        }

        if (builder.sinkContext != null) {
            typedArgumentsSuppliers.put(SinkContext.class, () -> builder.sinkContext);
        }

        typedArgumentsSuppliers.put(CircuitBreaker.class, () -> builder.circuitBreaker);
    }

    @Override
    public Object[] createArguments(final Class<?>[] parameterTypes, final Object ... args) {
        Map<Class<?>, Supplier<Object>> optionalArgumentsSuppliers = new HashMap<>();
        for (final Object arg: args) {
            if (Objects.nonNull(arg)) {
                optionalArgumentsSuppliers.put(arg.getClass(), () -> arg);
                for (final Class interfaceClass: arg.getClass().getInterfaces()) {
                    optionalArgumentsSuppliers.put(interfaceClass, () -> arg);
                }
            }
        }
        return Arrays.stream(parameterTypes)
                .map(parameterType -> getRequiredArgumentSupplier(parameterType, optionalArgumentsSuppliers))
                .map(Supplier::get)
                .toArray();
    }

    private Supplier<Object> getRequiredArgumentSupplier(final Class<?> parameterType, Map<Class<?>, Supplier<Object>> optionalArgumentsSuppliers) {
        if(typedArgumentsSuppliers.containsKey(parameterType)) {
            return typedArgumentsSuppliers.get(parameterType);
        } else if(optionalArgumentsSuppliers.containsKey(parameterType)) {
            return optionalArgumentsSuppliers.get(parameterType);
        } else if (beanFactory != null) {
            return createBeanSupplier(parameterType, beanFactory);
        } else {
            throw new InvalidPluginDefinitionException(UNABLE_TO_CREATE_PLUGIN_PARAMETER + parameterType);
        }
    }

    /**
     * @since 1.3
     *
     * Create a supplier to return a bean of type <pre>parameterType</pre> if one is available in <pre>beanFactory</pre>
     *
     * @param parameterType type of bean requested
     * @param beanFactory bean source the generated supplier will use
     * @return supplier of object type bean
     * @throws InvalidPluginDefinitionException if no bean is available from beanFactory
     */
    private <T> Supplier<T> createBeanSupplier(final Class<? extends T> parameterType, final BeanFactory beanFactory) {
        return () -> {
            try {
                return beanFactory.getBean(parameterType);
            } catch (final BeansException e) {
                throw new InvalidPluginDefinitionException(UNABLE_TO_CREATE_PLUGIN_PARAMETER + parameterType, e);
            }
        };
    }

    static class Builder {
        private Object pluginConfiguration;
        private PluginSetting pluginSetting;
        private PluginFactory pluginFactory;
        private PipelineDescription pipelineDescription;
        private BeanFactory beanFactory;
        private EventFactory eventFactory;
        private AcknowledgementSetManager acknowledgementSetManager;
        private PluginConfigObservable pluginConfigObservable;
        private SinkContext sinkContext;
        private CircuitBreaker circuitBreaker;

        Builder withPluginConfiguration(final Object pluginConfiguration) {
            this.pluginConfiguration = pluginConfiguration;
            return this;
        }

        Builder withPluginSetting(final PluginSetting pluginSetting) {
            this.pluginSetting = pluginSetting;
            return this;
        }

        Builder withEventFactory(final EventFactory eventFactory) {
            this.eventFactory = eventFactory;
            return this;
        }

        Builder withAcknowledgementSetManager(final AcknowledgementSetManager acknowledgementSetManager) {
            this.acknowledgementSetManager = acknowledgementSetManager;
            return this;
        }

        Builder withPluginFactory(final PluginFactory pluginFactory) {
            this.pluginFactory = pluginFactory;
            return this;
        }

        Builder withSinkContext(final SinkContext sinkContext) {
            this.sinkContext = sinkContext;
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

        Builder withPluginConfigurationObservable(final PluginConfigObservable pluginConfigObservable) {
            this.pluginConfigObservable = pluginConfigObservable;
            return this;
        }

        Builder withCircuitBreaker(final CircuitBreaker circuitBreaker) {
            this.circuitBreaker = circuitBreaker;
            return this;
        }

        ComponentPluginArgumentsContext build() {
            return new ComponentPluginArgumentsContext(this);
        }
    }
}
