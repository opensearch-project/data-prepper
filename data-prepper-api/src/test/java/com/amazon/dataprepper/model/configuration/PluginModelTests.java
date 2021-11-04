package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;

public class PluginModelTests {

    private static class PluginHolder {
        private PluginModel singlePlugin;
        private List<PluginModel> listOfPlugins;

        public PluginModel getSinglePlugin() {
            return singlePlugin;
        }

        public List<PluginModel> getListOfPlugins() {
            return listOfPlugins;
        }

        public void setSinglePlugin(PluginModel singlePlugin) {
            this.singlePlugin = singlePlugin;
        }

        public void setListOfPlugins(List<PluginModel> listOfPlugins) {
            this.listOfPlugins = listOfPlugins;
        }
    }

    @Test
    public final void testSerializingPluginModel_noExceptions() throws JsonGenerationException, JsonMappingException, IOException {
        final PluginModel pluginModel = new PluginModel("customPlugin", new HashMap<>());
        final String serialized = new ObjectMapper().writeValueAsString(pluginModel);
        assertThat(serialized, notNullValue());
        assertThat(pluginModel.getPluginName(), notNullValue());
        assertThat(pluginModel.getPluginSettings(), notNullValue());
    }

    @Test
    public final void testSerialization_empty_plugin_to_YAML() throws JsonGenerationException, JsonMappingException, IOException {
        final PluginModel pluginModel = new PluginModel("customPlugin", new HashMap<>());

        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        final String serialized = mapper.writeValueAsString(pluginModel);

        InputStream inputStream = PluginModelTests.class.getResourceAsStream("/empty_plugin_serialized.yml");

        assertThat(serialized, notNullValue());
        assertThat(serialized, equalTo(convertInputStreamToString(inputStream)));
    }

    @Test
    public final void testUsingCustomSerializerWithPluginSettings_noExceptions() throws JsonGenerationException, JsonMappingException, IOException {
        final PluginModel pluginModel = new PluginModel("customPlugin", validPluginSettings());

        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        final String serialized = mapper.writeValueAsString(pluginModel);

        InputStream inputStream = PluginModelTests.class.getResourceAsStream("/serialized_with_plugin_settings.yml");
        assertThat(serialized, notNullValue());
        assertThat(serialized, equalTo(convertInputStreamToString(inputStream)));
    }

    @Test
    public final void testUsingCustomDeserializer_noExceptions() throws JsonParseException, JsonMappingException, IOException {
        InputStream inputStream = PluginModelTests.class.getResourceAsStream("/single_plugin.yml");

        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        final PluginHolder readValue = mapper.readValue(inputStream, PluginHolder.class);
        assertThat(readValue, notNullValue());
        assertThat(readValue.singlePlugin.getPluginName(), equalTo("pluginName"));
        assertThat(readValue.singlePlugin.getPluginSettings(), notNullValue());
        assertThat(readValue.singlePlugin.getPluginSettings(), hasKey("key1"));
        assertThat(readValue.singlePlugin.getPluginSettings().get("key1"), equalTo("value1"));
    }

    @Test
    public final void testUsingCustomDeserializer_with_array() throws JsonParseException, JsonMappingException, IOException {
        InputStream inputStream = PluginModelTests.class.getResourceAsStream("/list_of_plugins.yml");

        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        final PluginHolder readValue = mapper.readValue(inputStream, PluginHolder.class);
        assertThat(readValue, notNullValue());
        assertThat(readValue.listOfPlugins, notNullValue());
        assertThat(readValue.listOfPlugins.size(), equalTo(2));
        assertThat(readValue.listOfPlugins.get(0).getPluginName(), equalTo("customPluginA"));
        assertThat(readValue.listOfPlugins.get(0).getPluginSettings(), notNullValue());
        assertThat(readValue.listOfPlugins.get(0).getPluginSettings(), hasKey("key1"));
        assertThat(readValue.listOfPlugins.get(0).getPluginSettings().get("key1"), equalTo("value1"));
        assertThat(readValue.listOfPlugins.get(1).getPluginName(), equalTo("customPluginB"));
        assertThat(readValue.listOfPlugins.get(1).getPluginSettings(), notNullValue());
        assertThat(readValue.listOfPlugins.get(1).getPluginSettings(), hasKey("key2"));
        assertThat(readValue.listOfPlugins.get(1).getPluginSettings().get("key2"), equalTo("value2"));
    }

    public static Map<String, Object> validPluginSettings() {
        final Map<String, Object> settings = new HashMap<>();
        settings.put("key1", "value1");
        settings.put("key2", "value2");
        settings.put("key3", "value3");
        return settings;
    }

    public static String convertInputStreamToString(InputStream inputStream) {
        StringBuilder stringBuilder = new StringBuilder();
        try (Reader reader = new BufferedReader(new InputStreamReader(inputStream, Charset.forName(StandardCharsets.UTF_8.name())))) {
            int counter = 0;
            while ((counter = reader.read()) != -1) {
                stringBuilder.append((char) counter);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return stringBuilder.toString();
    }

}
