/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugin;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.annotations.UsesDataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;

import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NestedPluginDeserializerTest {

    @Mock
    private PluginFactory pluginFactory;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        final SimpleModule module = new SimpleModule("TestModule");
        module.setDeserializerModifier(new DataPrepperPluginBeanDeserializerModifier());
        objectMapper = new ObjectMapper().registerModule(module);
    }

    interface TestPlugin {
        String getValue();
    }

    static class ConfigWithPlugin {
        @JsonProperty("name")
        private String name;

        @JsonProperty("my_plugin")
        @UsesDataPrepperPlugin(pluginType = TestPlugin.class)
        private TestPlugin myPlugin;

        public String getName() {
            return name;
        }

        public TestPlugin getMyPlugin() {
            return myPlugin;
        }
    }

    @Nested
    class WithPluginFactoryPresent {
        private ObjectReader reader;

        @BeforeEach
        void setUp() {
            reader = objectMapper.readerFor(ConfigWithPlugin.class)
                    .withAttribute(NestedPluginDeserializer.PLUGIN_FACTORY_ATTRIBUTE_KEY, pluginFactory);
        }

        @Test
        void deserializes_plugin_with_settings_map() throws JsonProcessingException {
            final String name = UUID.randomUUID().toString();
            final String pluginName = UUID.randomUUID().toString();
            final String settingKey = UUID.randomUUID().toString();
            final String settingValue = UUID.randomUUID().toString();
            final String pluginValue = UUID.randomUUID().toString();
            final TestPlugin mockPlugin = () -> pluginValue;

            when(pluginFactory.loadPlugin(eq(TestPlugin.class), any(PluginSetting.class)))
                    .thenReturn(mockPlugin);

            final String json = "{\"name\": \"" + name + "\", \"my_plugin\": {\"" + pluginName +
                    "\": {\"" + settingKey + "\": \"" + settingValue + "\"}}}";

            final ConfigWithPlugin result = reader.readValue(json);

            assertThat(result.getMyPlugin(), sameInstance(mockPlugin));

            final ArgumentCaptor<PluginSetting> captor = ArgumentCaptor.forClass(PluginSetting.class);
            verify(pluginFactory).loadPlugin(eq(TestPlugin.class), captor.capture());
            assertThat(captor.getValue().getName(), equalTo(pluginName));
            assertThat(captor.getValue().getSettings().get(settingKey), equalTo(settingValue));
        }

        @Test
        void deserializes_plugin_with_empty_settings() throws JsonProcessingException {
            final String name = UUID.randomUUID().toString();
            final String pluginName = UUID.randomUUID().toString();
            final String pluginValue = UUID.randomUUID().toString();
            final TestPlugin mockPlugin = () -> pluginValue;

            when(pluginFactory.loadPlugin(eq(TestPlugin.class), any(PluginSetting.class)))
                    .thenReturn(mockPlugin);

            final String json = "{\"name\": \"" + name + "\", \"my_plugin\": {\"" + pluginName + "\": {}}}";

            final ConfigWithPlugin result = reader.readValue(json);

            assertThat(result.getMyPlugin(), sameInstance(mockPlugin));

            final ArgumentCaptor<PluginSetting> captor = ArgumentCaptor.forClass(PluginSetting.class);
            verify(pluginFactory).loadPlugin(eq(TestPlugin.class), captor.capture());
            assertThat(captor.getValue().getName(), equalTo(pluginName));
            assertThat(captor.getValue().getSettings(), notNullValue());
            assertThat(captor.getValue().getSettings().isEmpty(), equalTo(true));
        }

        @Test
        void deserializes_plugin_with_null_settings() throws JsonProcessingException {
            final String name = UUID.randomUUID().toString();
            final String pluginName = UUID.randomUUID().toString();
            final String pluginValue = UUID.randomUUID().toString();
            final TestPlugin mockPlugin = () -> pluginValue;

            when(pluginFactory.loadPlugin(eq(TestPlugin.class), any(PluginSetting.class)))
                    .thenReturn(mockPlugin);

            final String json = "{\"name\": \"" + name + "\", \"my_plugin\": {\"" + pluginName + "\": null}}";

            final ConfigWithPlugin result = reader.readValue(json);

            assertThat(result.getMyPlugin(), sameInstance(mockPlugin));

            final ArgumentCaptor<PluginSetting> captor = ArgumentCaptor.forClass(PluginSetting.class);
            verify(pluginFactory).loadPlugin(eq(TestPlugin.class), captor.capture());
            assertThat(captor.getValue().getName(), equalTo(pluginName));
            assertThat(captor.getValue().getSettings(), equalTo(Collections.emptyMap()));
        }

        @Test
        void returns_null_when_field_is_null_in_json() throws JsonProcessingException {
            final String name = UUID.randomUUID().toString();
            final String json = "{\"name\": \"" + name + "\", \"my_plugin\": null}";

            final ConfigWithPlugin result = reader.readValue(json);

            assertThat(result.getName(), equalTo(name));
            assertThat(result.getMyPlugin(), nullValue());
        }

        @Test
        void returns_null_when_field_is_absent() throws JsonProcessingException {
            final String name = UUID.randomUUID().toString();
            final String json = "{\"name\": \"" + name + "\"}";

            final ConfigWithPlugin result = reader.readValue(json);

            assertThat(result.getName(), equalTo(name));
            assertThat(result.getMyPlugin(), nullValue());
        }

        @Test
        void throws_when_plugin_value_is_not_an_object() {
            final String name = UUID.randomUUID().toString();
            final String json = "{\"name\": \"" + name + "\", \"my_plugin\": \"not_an_object\"}";

            final JsonMappingException exception = assertThrows(JsonMappingException.class,
                    () -> reader.readValue(json));

            assertThat(exception.getMessage(), containsString("Nested plugin configuration must be a map"));
        }

        @Test
        void throws_when_plugin_value_is_empty_string() {
            final String name = UUID.randomUUID().toString();
            final String json = "{\"name\": \"" + name + "\", \"my_plugin\": \"\"}";

            final JsonMappingException exception = assertThrows(JsonMappingException.class,
                    () -> reader.readValue(json));

            assertThat(exception.getMessage(), containsString("Nested plugin configuration must be a map"));
        }

        @Test
        void throws_when_plugin_value_is_an_array() {
            final String name = UUID.randomUUID().toString();
            final String json = "{\"name\": \"" + name + "\", \"my_plugin\": [\"a\", \"b\"]}";

            final JsonMappingException exception = assertThrows(JsonMappingException.class,
                    () -> reader.readValue(json));

            assertThat(exception.getMessage(), containsString("Nested plugin configuration must be a map"));
        }

        @Test
        void throws_when_plugin_value_is_a_number() {
            final String name = UUID.randomUUID().toString();
            final String json = "{\"name\": \"" + name + "\", \"my_plugin\": 123}";

            final JsonMappingException exception = assertThrows(JsonMappingException.class,
                    () -> reader.readValue(json));

            assertThat(exception.getMessage(), containsString("Nested plugin configuration must be a map"));
        }

        @Test
        void uses_first_plugin_and_ignores_extra_keys_when_multiple_keys_present_for_backward_compatibility() throws JsonProcessingException {
            final String name = UUID.randomUUID().toString();
            final String firstPluginName = UUID.randomUUID().toString();
            final String secondPluginName = UUID.randomUUID().toString();
            final String pluginValue = UUID.randomUUID().toString();
            final TestPlugin mockPlugin = () -> pluginValue;

            when(pluginFactory.loadPlugin(eq(TestPlugin.class), any(PluginSetting.class)))
                    .thenReturn(mockPlugin);

            final String json = "{\"name\": \"" + name + "\", \"my_plugin\": {\"" + firstPluginName +
                    "\": {\"key\": \"val\"}, \"" + secondPluginName + "\": {\"other\": \"data\"}}}";

            final ConfigWithPlugin result = reader.readValue(json);

            assertThat(result.getMyPlugin(), sameInstance(mockPlugin));

            final ArgumentCaptor<PluginSetting> captor = ArgumentCaptor.forClass(PluginSetting.class);
            verify(pluginFactory).loadPlugin(eq(TestPlugin.class), captor.capture());
            assertThat(captor.getValue().getName(), equalTo(firstPluginName));
        }
    }

    @Nested
    class WithoutPluginFactory {
        private ObjectReader reader;

        @BeforeEach
        void setUp() {
            reader = objectMapper.readerFor(ConfigWithPlugin.class);
        }

        @Test
        void throws_when_plugin_factory_is_not_in_context() {
            final String name = UUID.randomUUID().toString();
            final String pluginName = UUID.randomUUID().toString();
            final String json = "{\"name\": \"" + name + "\", \"my_plugin\": {\"" + pluginName + "\": {}}}";

            final JsonMappingException exception = assertThrows(JsonMappingException.class,
                    () -> reader.readValue(json));

            assertThat(exception.getMessage(), containsString("PluginFactory is not available"));
        }
    }
}
