package org.opensearch.dataprepper.schemas;

import com.fasterxml.classmate.TypeBindings;
import com.fasterxml.classmate.types.ResolvedObjectType;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.FieldScope;
import com.github.victools.jsonschema.generator.Module;
import com.github.victools.jsonschema.generator.Option;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaGenerator;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfig;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigBuilder;
import com.github.victools.jsonschema.generator.SchemaGeneratorConfigPart;
import com.github.victools.jsonschema.generator.SchemaGeneratorGeneralConfigPart;
import com.github.victools.jsonschema.generator.SchemaKeyword;
import com.github.victools.jsonschema.generator.SchemaVersion;
import org.opensearch.dataprepper.model.annotations.AlsoRequired;
import org.opensearch.dataprepper.model.annotations.ConditionalRequired;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.UsesDataPrepperPlugin;
import org.opensearch.dataprepper.plugin.PluginProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class JsonSchemaConverter {
    private static final Logger LOG = LoggerFactory.getLogger(JsonSchemaConverter.class);
    static final String KEY_VALUE_PAIR_DELIMITER = ":";
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
        resolveDependentRequiresFields(scopeSchemaGeneratorConfigPart);
        overrideDataPrepperPluginTypeAttribute(configBuilder.forTypesInGeneral(), schemaVersion, optionPreset);
        overrideTypeAttributeWithConditionalRequired(configBuilder.forTypesInGeneral());
        resolveDataPrepperTypes(scopeSchemaGeneratorConfigPart);
        scopeSchemaGeneratorConfigPart.withInstanceAttributeOverride(new ExampleValuesInstanceAttributeOverride());

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

    private void overrideTypeAttributeWithConditionalRequired(
            final SchemaGeneratorGeneralConfigPart schemaGeneratorGeneralConfigPart) {
        schemaGeneratorGeneralConfigPart.withTypeAttributeOverride((node, scope, context) -> {
            final ConditionalRequired conditionalRequiredAnnotation = scope.getContext()
                    .getTypeAnnotationConsideringHierarchy(scope.getType(), ConditionalRequired.class);
            if (conditionalRequiredAnnotation != null) {
                final SchemaGeneratorConfig config = context.getGeneratorConfig();
                final ArrayNode ifThenElseArrayNode = node.putArray(config.getKeyword(SchemaKeyword.TAG_ALLOF));
                Arrays.asList(conditionalRequiredAnnotation.value()).forEach(ifThenElse -> {
                    ObjectNode ifThenElseNode = config.createObjectNode();
                    final ObjectNode ifObjectNode = constructIfObjectNode(config, ifThenElse.ifFulfilled());
                    ifThenElseNode.set(config.getKeyword(SchemaKeyword.TAG_IF), ifObjectNode);
                    final ObjectNode thenObjectNode = constructExpectObjectNode(config, ifThenElse.thenExpect());
                    ifThenElseNode.set(config.getKeyword(SchemaKeyword.TAG_THEN), thenObjectNode);
                    final ObjectNode elseObjectNode = constructExpectObjectNode(config, ifThenElse.elseExpect());
                    if (!elseObjectNode.isEmpty()) {
                        ifThenElseNode.set(config.getKeyword(SchemaKeyword.TAG_ELSE), elseObjectNode);
                    }
                    ifThenElseArrayNode.add(ifThenElseNode);
                });
            }
        });
    }

    private ObjectNode constructIfObjectNode(final SchemaGeneratorConfig config,
                                             final ConditionalRequired.SchemaProperty[] schemaProperties) {
        final ObjectNode ifObjectNode = config.createObjectNode();
        final ObjectNode ifPropertiesNode = ifObjectNode.putObject(config.getKeyword(SchemaKeyword.TAG_PROPERTIES));
        Arrays.asList(schemaProperties).forEach(schemaProperty -> {
            ifPropertiesNode.putObject(schemaProperty.field()).put(
                    config.getKeyword(SchemaKeyword.TAG_CONST), schemaProperty.value());
        });
        return ifObjectNode;
    }

    private ObjectNode constructExpectObjectNode(final SchemaGeneratorConfig config,
                                                 final ConditionalRequired.SchemaProperty[] schemaProperties) {
        final ObjectNode expectObjectNode = config.createObjectNode();
        final ObjectNode expectPropertiesNode = config.createObjectNode();
        final ArrayNode expectRequiredNode = config.createArrayNode();
        Arrays.asList(schemaProperties).forEach(schemaProperty -> {
            if (!Objects.equals(schemaProperty.value(), "")) {
                expectPropertiesNode.putObject(schemaProperty.field()).put(
                        config.getKeyword(SchemaKeyword.TAG_CONST), schemaProperty.value());
            } else {
                expectRequiredNode.add(schemaProperty.field());
            }
        });
        if (!expectPropertiesNode.isEmpty()) {
            expectObjectNode.set(config.getKeyword(SchemaKeyword.TAG_PROPERTIES), expectPropertiesNode);
        }
        if (!expectRequiredNode.isEmpty()) {
            expectObjectNode.set(config.getKeyword(SchemaKeyword.TAG_REQUIRED), expectRequiredNode);
        }
        return expectObjectNode;
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
                .ofNullable(field.getAnnotationConsideringFieldAndGetter(AlsoRequired.class))
                .map(alsoRequired -> Arrays.stream(alsoRequired.values())
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

    private void resolveDataPrepperTypes(final SchemaGeneratorConfigPart<FieldScope> scopeSchemaGeneratorConfigPart) {
        scopeSchemaGeneratorConfigPart.withTargetTypeOverridesResolver(field -> {
            if(field.getType().getErasedType().equals(EventKey.class)) {
                return Collections.singletonList(ResolvedObjectType.create(String.class, TypeBindings.emptyBindings(), null, null));
            }
            return Collections.singletonList(field.getType());
        });
    }
}
