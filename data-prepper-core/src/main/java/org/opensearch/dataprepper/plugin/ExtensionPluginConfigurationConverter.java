package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;

import javax.inject.Inject;
import javax.inject.Named;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Named
public class ExtensionPluginConfigurationConverter {
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

    public Object convert(final Class<?> extensionPluginConfigurationType, final String rootKey) {
        Objects.requireNonNull(extensionPluginConfigurationType);
        Objects.requireNonNull(rootKey);

        final Object configuration = convertSettings(extensionPluginConfigurationType,
                extensionPluginConfigurationResolver.getExtensionMap().get(rootKey));

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
}
