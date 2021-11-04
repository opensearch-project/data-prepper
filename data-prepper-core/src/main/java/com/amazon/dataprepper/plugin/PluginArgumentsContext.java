package com.amazon.dataprepper.plugin;

import com.amazon.dataprepper.metrics.PluginMetrics;
import com.amazon.dataprepper.model.configuration.PluginSetting;
import com.amazon.dataprepper.model.plugin.InvalidPluginDefinitionException;

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
    private final Map<Class<?>, Supplier<Object>> typedArgumentsSuppliers;

    private PluginArgumentsContext(final Builder builder) {
        Objects.requireNonNull(builder.pluginSetting,
                "PluginArgumentsContext received a null Builder object. This is likely an error in the plugin framework.");

        typedArgumentsSuppliers = new HashMap<>();

        typedArgumentsSuppliers.put(builder.pluginSetting.getClass(), () -> builder.pluginSetting);

        if(builder.pluginConfiguration != null) {
            typedArgumentsSuppliers.put(builder.pluginConfiguration.getClass(), () -> builder.pluginConfiguration);
        }

        typedArgumentsSuppliers.put(PluginMetrics.class, () -> PluginMetrics.fromPluginSetting(builder.pluginSetting));
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

        throw new InvalidPluginDefinitionException("Unable to create an argument for required plugin parameter type: " + parameterType);
    }

    static class Builder {
        private Object pluginConfiguration;
        private PluginSetting pluginSetting;

        Builder withPluginConfiguration(final Object pluginConfiguration) {
            this.pluginConfiguration = pluginConfiguration;
            return this;
        }

        Builder withPluginSetting(final PluginSetting pluginSetting) {
            this.pluginSetting = pluginSetting;
            return this;
        }

        PluginArgumentsContext build() {
            return new PluginArgumentsContext(this);
        }
    }
}
