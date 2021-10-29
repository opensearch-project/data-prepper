package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class PluginModelTests {

    @Test
    public final void testSerializingPluginModel_noExceptions() throws JsonGenerationException, JsonMappingException, IOException {
        final PluginModel pluginModel = new PluginModel("pluginName", new HashMap<>());
        final String serialized = new ObjectMapper().writeValueAsString(pluginModel);
        assertThat(serialized, notNullValue());
        assertThat(pluginModel.getPluginName(), notNullValue());
        assertThat(pluginModel.getPluginSettings(), notNullValue());
    }

    @Test
    public final void testUsingCustomSerializer_noExceptions() throws JsonGenerationException, JsonMappingException, IOException {
        final PluginModel pluginModel = new PluginModel("pluginName", new HashMap<>());

        final ObjectMapper mapper = new ObjectMapper();

        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(PluginModel.class, new PluginModel.PluginModelSerializer());
        mapper.registerModule(simpleModule);

        final String serialized = mapper.writeValueAsString(pluginModel);
        assertThat(serialized, notNullValue());
    }

    @Test
    public final void testUsingCustomSerializerWithPluginSettings_noExceptions() throws JsonGenerationException, JsonMappingException, IOException {
        final PluginModel pluginModel = new PluginModel("pluginName", validPluginSettings());

        final ObjectMapper mapper = new ObjectMapper();

        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(PluginModel.class, new PluginModel.PluginModelSerializer());
        mapper.registerModule(simpleModule);

        final String serialized = mapper.writeValueAsString(pluginModel);
        assertThat(serialized, notNullValue());
    }

    @Test
    public final void testDeserializingPluginModel_noExceptions() throws JsonParseException, JsonMappingException, IOException {
        final String yaml = "pluginName:\n key1: value1";
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        PluginModel readValue = mapper.readValue(yaml, PluginModel.class);
        assertThat(readValue, notNullValue());
    }

    @Test
    public final void testUsingCustomDeserializer_noExceptions() throws JsonParseException, JsonMappingException, IOException {
        final String yaml = "pluginName:\n key1: value1";
        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        final SimpleModule module = new SimpleModule();
        module.addDeserializer(PluginModel.class, new PluginModel.PluginModelDeserializer());
        mapper.registerModule(module);

        final PluginModel readValue = mapper.readValue(yaml, PluginModel.class);
        assertThat(readValue, notNullValue());
    }

    public static Map<String, Object> validPluginSettings() {
        final Map<String, Object> settings = new HashMap<>();
        settings.put("key1", "value1");
        settings.put("key2", "value2");
        settings.put("key3", "value3");
        return settings;
    }

}
