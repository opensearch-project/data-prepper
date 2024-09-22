package org.opensearch.dataprepper.schemas;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.github.victools.jsonschema.generator.Module;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaVersion;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationModule;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationOption;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.schemas.module.CustomJacksonModule;
import org.reflections.Reflections;
import org.reflections.scanners.Scanners;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.util.Collections;
import java.util.List;

import static com.github.victools.jsonschema.module.jackson.JacksonOption.RESPECT_JSONPROPERTY_ORDER;
import static com.github.victools.jsonschema.module.jackson.JacksonOption.RESPECT_JSONPROPERTY_REQUIRED;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.schemas.PluginConfigsJsonSchemaConverterIT.DEFAULT_PLUGINS_CLASSPATH;

@ExtendWith(MockitoExtension.class)
class JsonSchemaConverterTest {
    @Mock
    private Reflections reflections;

    public JsonSchemaConverter createObjectUnderTest(final List<Module> modules, final Reflections reflections) {
        return new JsonSchemaConverter(modules, reflections);
    }

    @Test
    void testConvertIntoJsonSchemaWithDefaultModules() throws JsonProcessingException {
        final JsonSchemaConverter jsonSchemaConverter = createObjectUnderTest(
                Collections.emptyList(), reflections);
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
                reflections);
        final ObjectNode jsonSchemaNode = jsonSchemaConverter.convertIntoJsonSchema(
                SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, TestConfig.class);
        assertThat(jsonSchemaNode, instanceOf(ObjectNode.class));
        assertThat(jsonSchemaNode.has("description"), is(true));
        final JsonNode propertiesNode = jsonSchemaNode.at("/properties");
        assertThat(propertiesNode, instanceOf(ObjectNode.class));
        assertThat(propertiesNode.has("test_attribute_with_getter"), is(true));
        assertThat(propertiesNode.has("custom_test_attribute"), is(true));
    }

    @JsonClassDescription("test config")
    static class TestConfig {
        private String testAttributeWithGetter;

        @JsonProperty("custom_test_attribute")
        private String testAttributeWithJsonPropertyAnnotation;

        @JsonProperty(defaultValue = "default_value")
        private String testAttributeWithDefaultValue;

        public String getTestAttributeWithGetter() {
            return testAttributeWithGetter;
        }
    }

    public static void main(String[] args) throws JsonProcessingException {
        final List<Module> modules = List.of(
                new CustomJacksonModule(RESPECT_JSONPROPERTY_REQUIRED, RESPECT_JSONPROPERTY_ORDER),
                new JakartaValidationModule(JakartaValidationOption.NOT_NULLABLE_FIELD_IS_REQUIRED,
                        JakartaValidationOption.INCLUDE_PATTERN_EXPRESSIONS)
        );
        final Reflections reflections = new Reflections(new ConfigurationBuilder()
                .setUrls(ClasspathHelper.forPackage(DEFAULT_PLUGINS_CLASSPATH))
                .setScanners(Scanners.TypesAnnotated, Scanners.SubTypes));
        JsonSchemaConverter jsonSchemaConverter = new JsonSchemaConverter(modules, reflections);
        System.out.println(jsonSchemaConverter.convertIntoJsonSchema(
                SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, TestPluginConfig.class).toPrettyString());
    }
}