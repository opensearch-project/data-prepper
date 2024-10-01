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
import com.github.victools.jsonschema.generator.SchemaGeneratorGeneralConfigPart;
import com.github.victools.jsonschema.generator.SchemaVersion;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.UsesDataPrepperPlugin;
import org.opensearch.dataprepper.plugin.PluginProvider;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class JsonSchemaConverter {
    private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaConverter.class);
    static final String DEPRECATED_SINCE_KEY = "deprecated";
    private final List<Module> jsonSchemaGeneratorModules;
    private final PluginProvider pluginProvider;

    public JsonSchemaConverter(final List<Module> jsonSchemaGeneratorModules, final PluginProvider pluginProvider) {
        this.jsonSchemaGeneratorModules = jsonSchemaGeneratorModules;
        this.pluginProvider = pluginProvider;
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
        overrideDataPrepperPluginTypeAttribute(configBuilder.forTypesInGeneral(), schemaVersion, optionPreset);

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
                .map(usesDataPrepperPlugin ->
                        pluginProvider.findPluginClasses(usesDataPrepperPlugin.pluginType()).stream())
                .map(stream -> stream.map(specificSubtype -> field.getContext().resolve(specificSubtype)))
                .map(stream -> stream.collect(Collectors.toList()))
                .orElse(null));
    }

    private void overrideDataPrepperPluginTypeAttribute(
            final SchemaGeneratorGeneralConfigPart schemaGeneratorGeneralConfigPart,
            final SchemaVersion schemaVersion, final OptionPreset optionPreset) {
        schemaGeneratorGeneralConfigPart.withTypeAttributeOverride((node, scope, context) -> {
            final DataPrepperPlugin dataPrepperPlugin = scope.getType().getErasedType()
                    .getAnnotation(DataPrepperPlugin.class);
            if (dataPrepperPlugin != null) {
                final ObjectNode propertiesNode = node.putObject("properties");
                try {
                    final ObjectNode schemaNode = this.convertIntoJsonSchema(
                            schemaVersion, optionPreset, dataPrepperPlugin.pluginConfigurationType());
                    propertiesNode.set(dataPrepperPlugin.name(), schemaNode);
                } catch (JsonProcessingException e) {
                    LOG.error("Encountered error retrieving JSON schema for {}", dataPrepperPlugin.name(), e);
                    throw new RuntimeException(e);
                }
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
}
