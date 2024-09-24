package org.opensearch.dataprepper.schemas;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.Module;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaVersion;
import com.github.victools.jsonschema.module.jackson.JacksonModule;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationModule;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationOption;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.annotations.UsesDataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.util.List;

import static com.github.victools.jsonschema.module.jackson.JacksonOption.RESPECT_JSONPROPERTY_REQUIRED;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class JsonSchemaConverterIT {
    static final String DEFAULT_PLUGINS_CLASSPATH = "org.opensearch.dataprepper.plugins";
    static final String PROPERTIES_KEY = "properties";
    static final String ANY_OF_KEY = "anyOf";

    private JsonSchemaConverter objectUnderTest;

    @BeforeEach
    void setUp() {
        final List<Module> modules = List.of(
                new JacksonModule(RESPECT_JSONPROPERTY_REQUIRED),
                new JakartaValidationModule(JakartaValidationOption.NOT_NULLABLE_FIELD_IS_REQUIRED,
                        JakartaValidationOption.INCLUDE_PATTERN_EXPRESSIONS)
        );
        final Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(DEFAULT_PLUGINS_CLASSPATH))
                .setScanners(Scanners.TypesAnnotated, Scanners.SubTypes));
        objectUnderTest = new JsonSchemaConverter(modules, reflections);
    }

    @Test
    void testSubTypes() throws JsonProcessingException {
        final ObjectNode jsonSchemaNode = objectUnderTest.convertIntoJsonSchema(
                SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, TestConfig.class);
        assertThat(jsonSchemaNode, instanceOf(ObjectNode.class));
        final JsonNode propertiesNode = jsonSchemaNode.at("/" + PROPERTIES_KEY);
        assertThat(propertiesNode, instanceOf(ObjectNode.class));
        assertThat(propertiesNode.has("action"), is(true));
        final JsonNode actionNode = propertiesNode.at("/action");
        assertThat(actionNode.has(ANY_OF_KEY), is(true));
        final JsonNode anyOfNode = actionNode.at("/" + ANY_OF_KEY);
        assertThat(anyOfNode, instanceOf(ArrayNode.class));
        anyOfNode.forEach(aggregateActionNode -> assertThat(aggregateActionNode.has(PROPERTIES_KEY), is(true)));
    }

    @JsonClassDescription("test config")
    static class TestConfig {
        @JsonPropertyDescription("The aggregate action description")
        @UsesDataPrepperPlugin(pluginType = AggregateAction.class)
        private PluginModel action;

        public PluginModel getAction() {
            return action;
        }
    }
}
