package org.opensearch.dataprepper.schemas;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.victools.jsonschema.generator.Module;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaVersion;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.model.annotations.AlsoRequired;
import org.opensearch.dataprepper.model.event.EventKey;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.plugin.PluginProvider;
import org.opensearch.dataprepper.schemas.module.CustomJacksonModule;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith(MockitoExtension.class)
class JsonSchemaConverterTest {
    @Mock
    private PluginProvider pluginProvider;

    public JsonSchemaConverter createObjectUnderTest(final List<Module> modules, final PluginProvider pluginProvider) {
        return new JsonSchemaConverter(modules, pluginProvider);
    }

    @Test
    void testConvertIntoJsonSchemaWithDefaultModules() throws JsonProcessingException {
        final JsonSchemaConverter jsonSchemaConverter = createObjectUnderTest(
                Collections.emptyList(), pluginProvider);
        final ObjectNode jsonSchemaNode = jsonSchemaConverter.convertIntoJsonSchema(
                SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, TestConfig.class);
        assertThat(jsonSchemaNode, instanceOf(ObjectNode.class));
        final JsonNode propertiesNode = jsonSchemaNode.at("/properties");
        assertThat(propertiesNode, instanceOf(ObjectNode.class));
        assertThat(propertiesNode.has("testAttributeWithDefaultValue"), is(true));
        final JsonNode testAttributeWithDefaultValueNode = propertiesNode.at("/testAttributeWithDefaultValue");
        assertThat(testAttributeWithDefaultValueNode, instanceOf(ObjectNode.class));
        assertThat(testAttributeWithDefaultValueNode.has("default"), is(true));
        assertThat(testAttributeWithDefaultValueNode.get("default"), equalTo(TextNode.valueOf("default_value")));
    }

    @Test
    void testConvertIntoJsonSchemaWithCustomJacksonModule() throws JsonProcessingException {
        final JsonSchemaConverter jsonSchemaConverter = createObjectUnderTest(
                Collections.singletonList(new CustomJacksonModule()),
                pluginProvider);
        final ObjectNode jsonSchemaNode = jsonSchemaConverter.convertIntoJsonSchema(
                SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, TestConfig.class);
        assertThat(jsonSchemaNode, instanceOf(ObjectNode.class));
        assertThat(jsonSchemaNode.has("description"), is(true));
        final JsonNode propertiesNode = jsonSchemaNode.at("/properties");
        assertThat(propertiesNode, instanceOf(ObjectNode.class));
        assertThat(propertiesNode.has("test_attribute_with_getter"), is(true));
        assertThat(propertiesNode.has("custom_test_attribute"), is(true));
        final JsonNode dependentRequiredNode = jsonSchemaNode.at("/dependentRequired");
        assertThat(dependentRequiredNode, instanceOf(ObjectNode.class));
        assertThat(dependentRequiredNode.has("test_mutually_exclusive_attribute_a"), is(true));
        assertThat(dependentRequiredNode.at("/test_mutually_exclusive_attribute_a"),
                instanceOf(ArrayNode.class));
        final ArrayNode dependentRequiredProperty1 = (ArrayNode) dependentRequiredNode.at(
                "/test_mutually_exclusive_attribute_a");
        assertThat(dependentRequiredProperty1.size(), equalTo(1));
        assertThat(dependentRequiredProperty1.get(0), equalTo(
                TextNode.valueOf("test_mutually_exclusive_attribute_b:[null, \"test_value\"]")));
        final ArrayNode dependentRequiredProperty2 = (ArrayNode) dependentRequiredNode.at(
                "/test_dependent_required_property_with_default_allowed_values");
        assertThat(dependentRequiredProperty2.size(), equalTo(1));
        assertThat(dependentRequiredProperty2.get(0), equalTo(
                TextNode.valueOf("test_mutually_exclusive_attribute_a")));
    }

    @Test
    void testConvertIntoJsonSchemaWithEventKey() throws JsonProcessingException {
        final JsonSchemaConverter jsonSchemaConverter = createObjectUnderTest(Collections.emptyList(), pluginProvider);
        final ObjectNode jsonSchemaNode = jsonSchemaConverter.convertIntoJsonSchema(
                SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, TestConfig.class);
        final JsonNode propertiesNode = jsonSchemaNode.at("/properties");
        assertThat(propertiesNode, instanceOf(ObjectNode.class));
        assertThat(propertiesNode.has("testAttributeEventKey"), is(equalTo(true)));
        assertThat(propertiesNode.get("testAttributeEventKey"), is(notNullValue()));
        assertThat(propertiesNode.get("testAttributeEventKey").get("type"), is(equalTo(TextNode.valueOf("string"))));
    }

    @JsonClassDescription("test config")
    static class TestConfig {
        private String testAttributeWithGetter;

        @JsonProperty("custom_test_attribute")
        private String testAttributeWithJsonPropertyAnnotation;

        @JsonProperty(defaultValue = "default_value")
        private String testAttributeWithDefaultValue;

        @JsonProperty
        @AlsoRequired(values = {
                @AlsoRequired.Required(name="test_mutually_exclusive_attribute_b", allowedValues = {"null", "\"test_value\""})
        })
        private String testMutuallyExclusiveAttributeA;

        private String testMutuallyExclusiveAttributeB;

        @JsonProperty
        @AlsoRequired(values = {
                @AlsoRequired.Required(name="test_mutually_exclusive_attribute_a")
        })
        private String testDependentRequiredPropertyWithDefaultAllowedValues;

        public String getTestAttributeWithGetter() {
            return testAttributeWithGetter;
        }

        private EventKey testAttributeEventKey;
    }
}