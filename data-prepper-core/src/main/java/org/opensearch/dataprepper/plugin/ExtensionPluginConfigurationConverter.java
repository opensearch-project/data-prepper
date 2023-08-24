package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.module.SimpleModule;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.parser.DataPrepperDurationDeserializer;
import org.opensearch.dataprepper.parser.model.DataPrepperConfiguration;

import javax.inject.Inject;
import javax.inject.Named;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Named
public class ExtensionPluginConfigurationConverter {
    private final DataPrepperConfiguration dataPrepperConfiguration;
    private final ObjectMapper objectMapper;
    private final Validator validator;

    @Inject
    public ExtensionPluginConfigurationConverter(final DataPrepperConfiguration dataPrepperConfiguration,
                                                 final Validator validator) {
        this.dataPrepperConfiguration = dataPrepperConfiguration;

        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addDeserializer(Duration.class, new DataPrepperDurationDeserializer());

        this.objectMapper = new ObjectMapper()
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
                .registerModule(simpleModule);

        this.validator = validator;
    }

    public Object convert(final Class<?> extensionPluginConfigurationType, final String rootKey) {
        Objects.requireNonNull(extensionPluginConfigurationType);
        Objects.requireNonNull(rootKey);

        final Map<String, Object> extensionProperties = dataPrepperConfiguration.getPipelineExtensions() == null?
                new HashMap<>() : dataPrepperConfiguration.getPipelineExtensions().getExtensionMap();

        final Object configuration = convertSettings(extensionPluginConfigurationType,
                extensionProperties.get(rootKey));

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
