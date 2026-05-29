/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import jakarta.validation.constraints.AssertTrue;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.springframework.context.annotation.DependsOn;

import javax.inject.Named;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Converts and validates a plugin configuration. This class is responsible for taking a {@link PluginSetting}
 * and converting it to the plugin model type which should be denoted by {@link DataPrepperPlugin#pluginConfigurationType()}
 */
@Named
@DependsOn({"extensionsApplier"})
class PluginConfigurationConverter {
    static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {};
    private final ObjectMapper objectMapper;
    private final Validator validator;

    private final PluginConfigurationErrorHandler pluginConfigurationErrorHandler;

    PluginConfigurationConverter(final Validator validator,
                                 @Named("pluginConfigObjectMapper")
                                 final ObjectMapper objectMapper,
                                 final PluginConfigurationErrorHandler pluginConfigurationErrorHandler) {
        this.objectMapper = objectMapper;
        this.validator = validator;
        this.pluginConfigurationErrorHandler = pluginConfigurationErrorHandler;
    }

    /**
     * Converts and validates to a plugin model type, resolving nested plugin fields
     * annotated with {@link org.opensearch.dataprepper.model.annotations.UsesDataPrepperPlugin}.
     *
     * @param pluginConfigurationType the destination type
     * @param pluginSetting           The source {@link PluginSetting}
     * @param pluginFactory           The {@link PluginFactory} for loading nested plugins
     * @return The converted object of type pluginConfigurationType
     * @throws InvalidPluginConfigurationException - If the plugin configuration is invalid
     */
    public Object convert(final Class<?> pluginConfigurationType, final PluginSetting pluginSetting,
                          final PluginFactory pluginFactory) {
        Objects.requireNonNull(pluginConfigurationType);
        Objects.requireNonNull(pluginSetting);
        Objects.requireNonNull(pluginFactory);

        if (pluginConfigurationType.equals(PluginSetting.class)) {
            final Map<String, Object> settings = pluginSetting.getSettings();
            final Map<String, Object> convertedSettings = objectMapper.convertValue(settings, MAP_TYPE_REFERENCE);
            pluginSetting.setSettings(convertedSettings);
            return pluginSetting;
        }

        final Object configuration = convertSettings(pluginConfigurationType, pluginSetting, pluginFactory);

        final Set<ConstraintViolation<Object>> constraintViolations = validator.validate(configuration);

        if (!constraintViolations.isEmpty()) {
            final String violationsString = constraintViolations.stream()
                    .map(this::constructConstrainViolationMessage)
                    .collect(Collectors.joining(". "));

            final String exceptionMessage = String.format("Plugin %s in pipeline %s is configured incorrectly: %s",
                    pluginSetting.getName(), pluginSetting.getPipelineName(), violationsString);
            throw new InvalidPluginConfigurationException(exceptionMessage);
        }

        return configuration;
    }

    private Object convertSettings(final Class<?> pluginConfigurationType, final PluginSetting pluginSetting,
                                   final PluginFactory pluginFactory) {
        Map<String, Object> settingsMap = pluginSetting.getSettings();
        if (settingsMap == null)
            settingsMap = Collections.emptyMap();

        try {
            final JsonNode tree = objectMapper.valueToTree(settingsMap);
            final ObjectReader reader = objectMapper.readerFor(pluginConfigurationType)
                    .withAttribute(NestedPluginDeserializer.PLUGIN_FACTORY_ATTRIBUTE_KEY, pluginFactory);
            final JsonParser parser = tree.traverse(objectMapper);
            return reader.readValue(parser);
        } catch (final Exception e) {
            throw pluginConfigurationErrorHandler.handleException(pluginSetting, e);
        }
    }

    private String constructConstrainViolationMessage(final ConstraintViolation<Object> constraintViolation) {
        if (constraintViolation.getConstraintDescriptor().getAnnotation().annotationType().equals(AssertTrue.class)) {
            return constraintViolation.getMessage();
        }

        return constraintViolation.getPropertyPath().toString() + " " + constraintViolation.getMessage();
    }

}
