package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class PluginModelDeserializationTest {

    @Test
    public final void whenDeserializing() throws JsonParseException, JsonMappingException, IOException {
        final String json = "{\"pluginName\":\"pluginName\"}, {\"pluginSettings\":\"pluginSettings\"}";
        PluginModel readValue = new ObjectMapper().readValue(json, PluginModel.class);
        assertThat(readValue, notNullValue());
    }

    @Test
    public final void whenDeserializingANonStandardRepresentation_thenCorrect() throws JsonParseException, JsonMappingException, IOException {
        final String json = "{\"pluginName\":\"source\"},{\"pluginSettings\":\"settings\"}";
        final ObjectMapper mapper = new ObjectMapper();

        final SimpleModule module = new SimpleModule();
        module.addDeserializer(PluginModel.class, new PluginModelDeserializer());
        mapper.registerModule(module);

        final PluginModel readValue = mapper.readValue(json, PluginModel.class);
        assertThat(readValue, notNullValue());
    }
}
