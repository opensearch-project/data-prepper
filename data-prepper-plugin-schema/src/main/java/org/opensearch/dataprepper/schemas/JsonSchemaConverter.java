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
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.UsesDataPrepperPlugin;
import org.reflections.Reflections;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JsonSchemaConverter {
    static final String DEPRECATED_SINCE_KEY = "deprecated";
    static final String PLUGIN_NAME_KEY = "name";
    private final List<Module> jsonSchemaGeneratorModules;
    private final Reflections reflections;

    public JsonSchemaConverter(final List<Module> jsonSchemaGeneratorModules, final Reflections reflections) {
        this.jsonSchemaGeneratorModules = jsonSchemaGeneratorModules;
        this.reflections = reflections;
    }

    public ObjectNode convertIntoJsonSchema(
            final SchemaVersion schemaVersion, final OptionPreset optionPreset, final Class<?> clazz)
            throws JsonProcessingException {
        final SchemaGeneratorConfigBuilder configBuilder = new SchemaGeneratorConfigBuilder(
                schemaVersion, optionPreset);
        loadJsonSchemaGeneratorModules(configBuilder);
        final SchemaGeneratorConfigPart<FieldScope> scopeSchemaGeneratorConfigPart = configBuilder.forFields();
        overrideInstanceAttributeWithDeprecated(scopeSchemaGeneratorConfigPart);
        overrideTargetTypeWithUsesDataPrepperPlugin(scopeSchemaGeneratorConfigPart);
        resolveDefaultValueFromJsonProperty(scopeSchemaGeneratorConfigPart);

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

    private void overrideTargetTypeWithUsesDataPrepperPlugin(
            final SchemaGeneratorConfigPart<FieldScope> scopeSchemaGeneratorConfigPart) {
        scopeSchemaGeneratorConfigPart.withTargetTypeOverridesResolver(field -> Optional
                .ofNullable(field.getAnnotationConsideringFieldAndGetterIfSupported(UsesDataPrepperPlugin.class))
                .map(usesDataPrepperPlugin -> scanForPluginConfigs(usesDataPrepperPlugin.pluginType()))
                .map(stream -> stream.map(specificSubtype -> field.getContext().resolve(specificSubtype)))
                .map(stream -> stream.collect(Collectors.toList()))
                .orElse(null));
    }

    private void resolveDefaultValueFromJsonProperty(
            final SchemaGeneratorConfigPart<FieldScope> scopeSchemaGeneratorConfigPart) {
        scopeSchemaGeneratorConfigPart.withDefaultResolver(field -> {
            final JsonProperty annotation = field.getAnnotationConsideringFieldAndGetter(JsonProperty.class);
            return annotation == null || annotation.defaultValue().isEmpty() ? null : annotation.defaultValue();
        });
    }

    private Stream<Class<?>> scanForPluginConfigs(final Class<?> pluginType) {
        final Stream<DataPrepperPlugin> result = reflections.getTypesAnnotatedWith(DataPrepperPlugin.class).stream()
                .map(clazz -> clazz.getAnnotation(DataPrepperPlugin.class))
                .filter(dataPrepperPlugin -> pluginType.equals(dataPrepperPlugin.pluginType()));
        return result.map(DataPrepperPlugin::pluginConfigurationType);
    }
}
