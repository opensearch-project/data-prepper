package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class PluginModelSerializationTest {

    @Test
    public final void whenSerializingPluginModel_noExceptions() throws JsonGenerationException, JsonMappingException, IOException {
        final PluginModel pluginModel = new PluginModel("pluginName", new HashMap<>());
        final String serialized = new ObjectMapper().writeValueAsString(pluginModel);
        assertThat(serialized, notNullValue());
        assertThat(pluginModel.getPluginName(), notNullValue());
        assertThat(pluginModel.getPluginSettings(), notNullValue());
    }

    @Test
    public final void whenUsingCustomSerializer_noExceptions() throws JsonGenerationException, JsonMappingException, IOException {
        final PluginModel pluginModel = new PluginModel("pluginName", new HashMap<>());

        final ObjectMapper mapper = new ObjectMapper();

        final SimpleModule simpleModule = new SimpleModule();
        simpleModule.addSerializer(PluginModel.class, new PluginModelSerializer());
        mapper.registerModule(simpleModule);

        final String serialized = mapper.writeValueAsString(pluginModel);
        assertThat(serialized, notNullValue());
    }
}
