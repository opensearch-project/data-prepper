package org.opensearch.dataprepper.schemas;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.victools.jsonschema.generator.OptionPreset;
import com.github.victools.jsonschema.generator.SchemaVersion;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.plugin.PluginProvider;

import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.dataprepper.schemas.PluginConfigsJsonSchemaConverter.DOCUMENTATION_LINK_KEY;
import static org.opensearch.dataprepper.schemas.PluginConfigsJsonSchemaConverter.PLUGIN_NAME_KEY;
import static org.opensearch.dataprepper.schemas.PluginConfigsJsonSchemaConverter.PLUGIN_TYPE_NAME_TO_CLASS_MAP;

@ExtendWith(MockitoExtension.class)
class PluginConfigsJsonSchemaConverterTest {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static final TypeReference<Map<String, Object>> MAP_TYPE_REFERENCE = new TypeReference<>() {};

    @Mock
    private JsonSchemaConverter jsonSchemaConverter;

    @Mock
    private PluginProvider pluginProvider;

    @InjectMocks
    private PluginConfigsJsonSchemaConverter objectUnderTest;

    @Test
    void testValidPluginTypeNames() {
        assertThat(PLUGIN_TYPE_NAME_TO_CLASS_MAP.keySet().containsAll(objectUnderTest.validPluginTypeNames()),
                is(true));
    }

    @Test
    void testPluginTypeNameToPluginTypeWithValidInput() {
        objectUnderTest.validPluginTypeNames().forEach(
                pluginType -> assertThat(objectUnderTest.pluginTypeNameToPluginType(pluginType),
                        equalTo(PLUGIN_TYPE_NAME_TO_CLASS_MAP.get(pluginType))));
    }

    @Test
    void testPluginTypeNameToPluginTypeWithInValidInput() {
        final String inValidPluginType = "invalid-" + UUID.randomUUID();
        assertThrows(
                IllegalArgumentException.class, () -> objectUnderTest.pluginTypeNameToPluginType(inValidPluginType));
    }

    @Test
    void testConvertPluginConfigsIntoJsonSchemasHappyPath() throws JsonProcessingException {
        when(pluginProvider.findPluginClasses(eq(TestPluginType.class))).thenReturn(Set.of(TestPlugin.class));
        final ObjectNode objectNode = OBJECT_MAPPER.createObjectNode();
        when(jsonSchemaConverter.convertIntoJsonSchema(
                any(SchemaVersion.class), any(OptionPreset.class), eq(TestPluginConfig.class))).thenReturn(objectNode);
        final Map<String, String> result = objectUnderTest.convertPluginConfigsIntoJsonSchemas(
                SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, TestPluginType.class);
        assertThat(result.size(), equalTo(1));
        final Map<String, Object> schemaMap = OBJECT_MAPPER.readValue(result.get("test_plugin"), MAP_TYPE_REFERENCE);
        assertThat(schemaMap, notNullValue());
        assertThat(schemaMap.get(DOCUMENTATION_LINK_KEY), equalTo(
                "{{site.url}}{{site.baseurl}}/data-prepper/pipelines/configuration/null/test_plugin/"
        ));
        assertThat(schemaMap.containsKey(PLUGIN_NAME_KEY), is(true));
    }

    @Test
    void testConvertPluginConfigsIntoJsonSchemasWithError() throws JsonProcessingException {
        when(pluginProvider.findPluginClasses(eq(TestPluginType.class))).thenReturn(Set.of(TestPlugin.class));
        final JsonProcessingException jsonProcessingException = mock(JsonProcessingException.class);
        when(jsonSchemaConverter.convertIntoJsonSchema(
                any(SchemaVersion.class), any(OptionPreset.class), eq(TestPluginConfig.class))).thenThrow(
                        jsonProcessingException);
        final Map<String, String> result = objectUnderTest.convertPluginConfigsIntoJsonSchemas(
                SchemaVersion.DRAFT_2020_12, OptionPreset.PLAIN_JSON, TestPluginType.class);
        assertThat(result.isEmpty(), is(true));
    }

    @DataPrepperPlugin(
            name = "test_plugin", pluginType = TestPluginType.class, pluginConfigurationType = TestPluginConfig.class)
    static class TestPlugin extends TestPluginType {

    }

    static class TestPluginConfig {

    }

    static class TestPluginType {

    }
}