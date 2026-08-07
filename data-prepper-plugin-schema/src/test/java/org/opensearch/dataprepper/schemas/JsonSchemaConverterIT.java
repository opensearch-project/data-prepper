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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.UsesDataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.plugin.ClasspathPluginProvider;
import org.opensearch.dataprepper.plugin.PluginProvider;
import org.opensearch.dataprepper.plugins.processor.aggregate.AggregateAction;
import org.opensearch.dataprepper.plugins.processor.grok.GrokProcessorConfig;

import java.util.List;
import java.util.Map;

import static com.github.victools.jsonschema.module.jackson.JacksonOption.RESPECT_JSONPROPERTY_REQUIRED;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

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

    @Nested
    class UseDefinitionsTests {
        private ObjectNode jsonSchemaNode;

        @BeforeEach
        void setUp() throws JsonProcessingException {
            final JsonSchemaConverterConfig config = new JsonSchemaConverterConfig(true);
            final JsonSchemaConverter objectUnderTestWithDefinitions = new JsonSchemaConverter(
                    List.of(new JacksonModule(RESPECT_JSONPROPERTY_REQUIRED),
                            new JakartaValidationModule(JakartaValidationOption.NOT_NULLABLE_FIELD_IS_REQUIRED,
                                    JakartaValidationOption.INCLUDE_PATTERN_EXPRESSIONS)),
                    new ClasspathPluginProvider(), config);

            jsonSchemaNode = objectUnderTestWithDefinitions.convertIntoJsonSchema(
                    SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, TestConfigWithNestedObject.class);
        }

        @Test
        void verify_the_definition_is_created() {
            assertThat(jsonSchemaNode.has("$defs"), is(true));
            final JsonNode defsNode = jsonSchemaNode.get("$defs");
            assertThat(defsNode, instanceOf(ObjectNode.class));
            assertThat(defsNode.has("NestedObject"), is(true));
        }

        @Test
        void verify_the_definition_has_the_correct_fields() {
            final JsonNode defsNode = jsonSchemaNode.get("$defs");
            final JsonNode nestedObjectDef = defsNode.get("NestedObject");
            assertThat(nestedObjectDef.has("properties"), is(true));
            final JsonNode propertiesNode = nestedObjectDef.get("properties");
            assertThat(propertiesNode.has("field"), is(true));
        }

        @Test
        void verify_the_referencing_object_has_an_actual_reference_to_the_definition() {
            final JsonNode rootProperties = jsonSchemaNode.get("properties");
            assertThat(rootProperties.has("nested_list"), is(true));
            final JsonNode nestedListProperty = rootProperties.get("nested_list");
            assertThat(nestedListProperty.has("items"), is(true));
            final JsonNode itemsNode = nestedListProperty.get("items");
            assertThat(itemsNode.has("$ref"), is(true));
            assertThat(itemsNode.get("$ref").textValue(), equalTo("#/$defs/NestedObject"));
        }
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

    @Nested
    class MapTypeTests {
        @Test
        void testMapFieldsDoNotCreateDefinitions() throws JsonProcessingException {
            final ObjectNode jsonSchemaNode = objectUnderTest.convertIntoJsonSchema(
                    SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, TestConfigWithMapFields.class);

            if (jsonSchemaNode.has("$defs")) {
                final JsonNode defsNode = jsonSchemaNode.get("$defs");
                assertThat(defsNode.toString(), not(containsString("Map(")));
            }

            final JsonNode properties = jsonSchemaNode.get("properties");
            assertThat(properties.get("simple_map").get("type").asText(), is("object"));
            assertThat(properties.get("nested_map").get("type").asText(), is("object"));
        }

        @Test
        void testGrokProcessorMapFieldsAreInline() throws JsonProcessingException {
            final ObjectNode jsonSchemaNode = objectUnderTest.convertIntoJsonSchema(
                    SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, GrokProcessorConfig.class);

            final JsonNode matchField = jsonSchemaNode.get("properties").get("match");
            assertThat(matchField.get("type").asText(), is("object"));

            if (jsonSchemaNode.has("$defs")) {
                final JsonNode defsNode = jsonSchemaNode.get("$defs");
                assertThat(defsNode.toString(), not(containsString("Map(")));
            }
        }
    }

    @JsonClassDescription("Test config with map fields")
    static class TestConfigWithMapFields {
        @JsonProperty("simple_map")
        private Map<String, String> simpleMap;

        @JsonProperty("nested_map")
        private Map<String, List<String>> nestedMap;
    }

    @JsonClassDescription("test config with nested object")
    static class TestConfigWithNestedObject {
        @JsonProperty("nested_list")
        private List<NestedObject> nestedList;
    }

    static class NestedObject {
        @JsonProperty("field")
        private String field;
    }
}
