package org.opensearch.dataprepper.schemas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.victools.jsonschema.generator.Module;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaVersion;
import com.github.victools.jsonschema.module.jackson.JacksonModule;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationModule;
import com.github.victools.jsonschema.module.jakarta.validation.JakartaValidationOption;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opensearch.dataprepper.plugin.ClasspathPluginProvider;
import org.opensearch.dataprepper.plugin.PluginProvider;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static com.github.victools.jsonschema.module.jackson.JacksonOption.RESPECT_JSONPROPERTY_REQUIRED;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.opensearch.dataprepper.schemas.PluginConfigsJsonSchemaConverter.DOCUMENTATION_LINK_KEY;
import static org.opensearch.dataprepper.schemas.PluginConfigsJsonSchemaConverter.PLUGIN_NAME_KEY;

class PluginConfigsJsonSchemaConverterIT {
    static final String DEFAULT_PLUGINS_CLASSPATH = "org.opensearch.dataprepper.plugins";
    private static final String TEST_URL = String.format("https://%s/", UUID.randomUUID());
    private static final String TEST_BASE_URL = String.format("/%s", UUID.randomUUID());
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {};

    private PluginConfigsJsonSchemaConverter objectUnderTest;

    @BeforeEach
    void setUp() {
        final List<Module> modules = List.of(
                new JacksonModule(RESPECT_JSONPROPERTY_REQUIRED),
                new JakartaValidationModule(JakartaValidationOption.NOT_NULLABLE_FIELD_IS_REQUIRED,
                        JakartaValidationOption.INCLUDE_PATTERN_EXPRESSIONS)
        );
        final PluginProvider pluginProvider = new ClasspathPluginProvider();
        objectUnderTest = new PluginConfigsJsonSchemaConverter(
                pluginProvider, new JsonSchemaConverter(modules, pluginProvider), TEST_URL, TEST_BASE_URL);
    }

    @ParameterizedTest
    @MethodSource("getValidPluginTypes")
    void testConvertPluginConfigsIntoJsonSchemas(final Class<?> pluginType) {
        final Map<String, String> result = objectUnderTest.convertPluginConfigsIntoJsonSchemas(
                SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, pluginType);
        assertThat(result.isEmpty(), is(false));
        result.values().forEach(schema -> {
            final Map<String, Object> schemaMap;
            try {
                schemaMap = OBJECT_MAPPER.readValue(schema, MAP_TYPE_REFERENCE);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            assertThat(schemaMap, notNullValue());
            assertThat(schemaMap.containsKey(PLUGIN_NAME_KEY), is(true));
            assertThat(((String) schemaMap.get(DOCUMENTATION_LINK_KEY)).startsWith(TEST_URL + TEST_BASE_URL),
                    is(true));
        });
    }

    private static Stream<Arguments> getValidPluginTypes() {
        return PluginConfigsJsonSchemaConverter.PLUGIN_TYPE_TO_URI_PARAMETER_MAP.keySet()
                .stream().flatMap(clazz -> Stream.of(Arguments.of(clazz)));
    }
}