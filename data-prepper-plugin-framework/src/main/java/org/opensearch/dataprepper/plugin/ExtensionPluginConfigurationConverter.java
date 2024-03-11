/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Named
public class ExtensionPluginConfigurationConverter {
    static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {};
    private final ExtensionPluginConfigurationResolver extensionPluginConfigurationResolver;
    private final ObjectMapper objectMapper;
    private final Validator validator;

    @Inject
    public ExtensionPluginConfigurationConverter(
            final ExtensionPluginConfigurationResolver extensionPluginConfigurationResolver,
            final Validator validator,
            @Named("extensionPluginConfigObjectMapper")
            final ObjectMapper objectMapper) {
        this.extensionPluginConfigurationResolver = extensionPluginConfigurationResolver;
        this.objectMapper = objectMapper;
        this.validator = validator;
    }

    public Object convert(final boolean configAllowedInPipelineConfigurations,
                          final Class<?> extensionPluginConfigurationType, final String rootKey) {
        Objects.requireNonNull(extensionPluginConfigurationType);
        Objects.requireNonNull(rootKey);

        final Object configuration = configAllowedInPipelineConfigurations ?
                convertSettings(extensionPluginConfigurationType,
                        getExtensionPluginConfigMap(
                                extensionPluginConfigurationResolver.getCombinedExtensionMap(), rootKey)) :
                convertSettings(extensionPluginConfigurationType,
                        getExtensionPluginConfigMap(
                                extensionPluginConfigurationResolver.getDataPrepperConfigExtensionMap(), rootKey));

        final Set<ConstraintViolation<Object>> constraintViolations = configuration == null ? Collections.emptySet() :
                validator.validate(configuration);

        if (!constraintViolations.isEmpty()) {
            final String violationsString = constraintViolations.stream()
                    .map(v -> v.getPropertyPath().toString() + " " + v.getMessage())
                    .collect(Collectors.joining(". "));

            final String exceptionMessage = String.format("Extension %s in PipelineExtensions " +
                            "is configured incorrectly: %s", rootKey, violationsString);
            throw new InvalidPluginConfigurationException(exceptionMessage);
        }
        return configuration;
    }

    private Object convertSettings(final Class<?> extensionPluginConfigurationType, final Object extensionPlugin) {
        return objectMapper.convertValue(extensionPlugin, extensionPluginConfigurationType);
    }

    private Map<String, Object> getExtensionPluginConfigMap(
            final Map<String, Object> extensionMap, final String rootKey) {
        final JsonNode jsonNode = objectMapper.valueToTree(extensionMap);
        final JsonPointer jsonPointer = JsonPointer.compile(rootKey);
        final JsonNode extensionPluginConfigNode = jsonNode.at(jsonPointer);
        return objectMapper.convertValue(extensionPluginConfigNode, MAP_TYPE_REFERENCE);
    }
}
