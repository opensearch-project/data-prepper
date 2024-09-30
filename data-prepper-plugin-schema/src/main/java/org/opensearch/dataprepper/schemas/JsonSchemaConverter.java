package org.opensearch.dataprepper.schemas;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.FieldScope;
import com.github.victools.jsonschema.generator.Module;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigPart;
import com.github.victools.jsonschema.generator.SchemaVersion;
import org.opensearch.dataprepper.model.annotations.AlsoRequires;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class JsonSchemaConverter {
    static final String KEY_VALUE_PAIR_DELIMITER = ":";
    static final String DEPRECATED_SINCE_KEY = "deprecated";
    private final List<Module> jsonSchemaGeneratorModules;

    public JsonSchemaConverter(final List<Module> jsonSchemaGeneratorModules) {
        this.jsonSchemaGeneratorModules = jsonSchemaGeneratorModules;
    }

    public ObjectNode convertIntoJsonSchema(
            final SchemaVersion schemaVersion, final OptionPreset optionPreset, final Class<?> clazz)
            throws JsonProcessingException {
        final SchemaGeneratorConfigBuilder configBuilder = new SchemaGeneratorConfigBuilder(
                schemaVersion, optionPreset);
        loadJsonSchemaGeneratorModules(configBuilder);
        final SchemaGeneratorConfigPart<FieldScope> scopeSchemaGeneratorConfigPart = configBuilder.forFields();
        overrideInstanceAttributeWithDeprecated(scopeSchemaGeneratorConfigPart);
        resolveDefaultValueFromJsonProperty(scopeSchemaGeneratorConfigPart);
        resolveDependentRequiresFields(scopeSchemaGeneratorConfigPart);

        final SchemaGeneratorConfig config = configBuilder.build();
        final SchemaGenerator generator = new SchemaGenerator(config);

        return generator.generateSchema(clazz);
    }

    private void loadJsonSchemaGeneratorModules(final SchemaGeneratorConfigBuilder configBuilder) {
        jsonSchemaGeneratorModules.forEach(configBuilder::with);
    }

    private void overrideInstanceAttributeWithDeprecated(
            final SchemaGeneratorConfigPart<FieldScope> scopeSchemaGeneratorConfigPart) {
        scopeSchemaGeneratorConfigPart.withInstanceAttributeOverride((node, field, context) -> {
            final Deprecated deprecatedAnnotation = field.getAnnotationConsideringFieldAndGetter(
                    Deprecated.class);
            if (deprecatedAnnotation != null) {
                node.put(DEPRECATED_SINCE_KEY, deprecatedAnnotation.since());
            }
        });
    }

    private void resolveDefaultValueFromJsonProperty(
            final SchemaGeneratorConfigPart<FieldScope> scopeSchemaGeneratorConfigPart) {
        scopeSchemaGeneratorConfigPart.withDefaultResolver(field -> {
            final JsonProperty annotation = field.getAnnotationConsideringFieldAndGetter(JsonProperty.class);
            return annotation == null || annotation.defaultValue().isEmpty() ? null : annotation.defaultValue();
        });
    }

    private void resolveDependentRequiresFields(
            final SchemaGeneratorConfigPart<FieldScope> scopeSchemaGeneratorConfigPart) {
        scopeSchemaGeneratorConfigPart.withDependentRequiresResolver(field -> Optional
                .ofNullable(field.getAnnotationConsideringFieldAndGetter(AlsoRequires.class))
                .map(alsoRequires -> Arrays.stream(alsoRequires.values())
                        .map(required -> {
                            final String property = required.name();
                            final String[] allowedValues = required.allowedValues();
                            if (allowedValues.length == 0) {
                                return property;
                            }
                            return property + KEY_VALUE_PAIR_DELIMITER + Arrays.toString(allowedValues);
                        })
                        .collect(Collectors.toList()))
                .orElse(null));
    }
}
