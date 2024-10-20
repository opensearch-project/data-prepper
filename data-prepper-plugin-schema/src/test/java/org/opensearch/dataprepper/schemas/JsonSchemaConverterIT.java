package org.opensearch.dataprepper.schemas;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.Module;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaVersion;
import com.github.victools.jsonschema.module.jackson.JacksonModule;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationModule;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationOption;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.UsesDataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugin.ClasspathPluginProvider;
import org.opensearch.dataprepper.plugin.PluginProvider;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;

import java.util.List;

import static com.github.victools.jsonschema.module.jackson.JacksonOption.RESPECT_JSONPROPERTY_REQUIRED;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class JsonSchemaConverterIT {
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
        final PluginProvider pluginProvider = new ClasspathPluginProvider();
        objectUnderTest = new JsonSchemaConverter(modules, pluginProvider);
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

    @Test
    void test_examples() throws JsonProcessingException {
        final ObjectNode jsonSchemaNode = objectUnderTest.convertIntoJsonSchema(
                SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, TestConfig.class);
        assertThat(jsonSchemaNode, instanceOf(ObjectNode.class));
        final JsonNode propertiesNode = jsonSchemaNode.at("/" + PROPERTIES_KEY);
        assertThat(propertiesNode, instanceOf(ObjectNode.class));
        assertThat(propertiesNode.has("string_value_with_two_examples"), is(true));
        final JsonNode propertyNode = propertiesNode.at("/string_value_with_two_examples");
        assertThat(propertyNode.has("type"), equalTo(true));
        assertThat(propertyNode.get("type").getNodeType(), equalTo(JsonNodeType.STRING));

        assertThat(propertyNode.has("examples"), equalTo(true));
        assertThat(propertyNode.get("examples").getNodeType(), equalTo(JsonNodeType.ARRAY));
        assertThat(propertyNode.get("examples").size(), equalTo(2));
        assertThat(propertyNode.get("examples").get(0), notNullValue());
        assertThat(propertyNode.get("examples").get(0).getNodeType(), equalTo(JsonNodeType.OBJECT));
        assertThat(propertyNode.get("examples").get(0).has("example"), equalTo(true));
        assertThat(propertyNode.get("examples").get(0).get("example").getNodeType(), equalTo(JsonNodeType.STRING));
        assertThat(propertyNode.get("examples").get(0).get("example").textValue(), equalTo("some example value"));
        assertThat(propertyNode.get("examples").get(0).has("description"), equalTo(false));

        assertThat(propertyNode.get("examples").get(1), notNullValue());
        assertThat(propertyNode.get("examples").get(1).getNodeType(), equalTo(JsonNodeType.OBJECT));
        assertThat(propertyNode.get("examples").get(1).has("example"), equalTo(true));
        assertThat(propertyNode.get("examples").get(1).get("example").getNodeType(), equalTo(JsonNodeType.STRING));
        assertThat(propertyNode.get("examples").get(1).get("example").textValue(), equalTo("second example value"));
        assertThat(propertyNode.get("examples").get(1).has("description"), equalTo(true));
        assertThat(propertyNode.get("examples").get(1).get("description").getNodeType(), equalTo(JsonNodeType.STRING));
        assertThat(propertyNode.get("examples").get(1).get("description").textValue(), equalTo("This is the second value."));
    }

    @JsonClassDescription("test config")
    static class TestConfig {
        @JsonPropertyDescription("The aggregate action description")
        @UsesDataPrepperPlugin(pluginType = AggregateAction.class)
        private PluginModel action;

        public PluginModel getAction() {
            return action;
        }

        @JsonProperty("string_value_with_two_examples")
        @ExampleValues({
                @ExampleValues.Example("some example value"),
                @ExampleValues.Example(value = "second example value", description = "This is the second value.")
        })
        private String stringValueWithTwoExamples;

        public String getStringValueWithTwoExamples() {
            return stringValueWithTwoExamples;
        }
    }
}
