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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.annotations.UsesDataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;

import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DataPrepperPluginBeanDeserializerModifierTest {

    @Mock
    private PluginFactory pluginFactory;

    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        final SimpleModule module = new SimpleModule("TestNestedPluginModule");
        module.setDeserializerModifier(new DataPrepperPluginBeanDeserializerModifier());
        objectMapper = new ObjectMapper().registerModule(module);
    }

    interface TestPlugin {
        String getValue();
    }

    static class ConfigWithNoAnnotatedFields {
        @JsonProperty("name")
        private String name;

        @JsonProperty("count")
        private int count;

        public String getName() {
            return name;
        }

        public int getCount() {
            return count;
        }
    }

    static class ConfigWithAnnotatedField {
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

    interface InnerPlugin {
        String getInnerValue();
    }

    static class ConfigWithNestedPluginThatAlsoLoadsPlugin {
        @JsonProperty("name")
        private String name;

        @JsonProperty("outer_plugin")
        @UsesDataPrepperPlugin(pluginType = TestPlugin.class)
        private TestPlugin outerPlugin;

        public String getName() {
            return name;
        }

        public TestPlugin getOuterPlugin() {
            return outerPlugin;
        }
    }

    static class OuterPluginConfig {
        @JsonProperty("setting")
        private String setting;

        @JsonProperty("inner_plugin")
        @UsesDataPrepperPlugin(pluginType = InnerPlugin.class)
        private InnerPlugin innerPlugin;

        public String getSetting() {
            return setting;
        }

        public InnerPlugin getInnerPlugin() {
            return innerPlugin;
        }
    }

    static class ConfigWithPluginModelField {
        @JsonProperty("name")
        private String name;

        @JsonProperty("action")
        @UsesDataPrepperPlugin(pluginType = TestPlugin.class)
        private PluginModel action;

        public String getName() {
            return name;
        }

        public PluginModel getAction() {
            return action;
        }
    }

    static class ConfigWithMultipleFields {
        @JsonProperty("label")
        private String label;

        @JsonProperty("first_plugin")
        @UsesDataPrepperPlugin(pluginType = TestPlugin.class)
        private TestPlugin firstPlugin;

        @JsonProperty("description")
        private String description;

        @JsonProperty("second_plugin")
        @UsesDataPrepperPlugin(pluginType = TestPlugin.class)
        private TestPlugin secondPlugin;

        public String getLabel() {
            return label;
        }

        public TestPlugin getFirstPlugin() {
            return firstPlugin;
        }

        public String getDescription() {
            return description;
        }

        public TestPlugin getSecondPlugin() {
            return secondPlugin;
        }
    }

    @Nested
    class WithNoAnnotatedFields {
        private ObjectReader reader;

        @BeforeEach
        void setUp() {
            reader = objectMapper.readerFor(ConfigWithNoAnnotatedFields.class)
                    .withAttribute(NestedPluginDeserializer.PLUGIN_FACTORY_ATTRIBUTE_KEY, pluginFactory);
        }

        @Test
        void does_not_invoke_plugin_factory() throws JsonProcessingException {
            final String name = UUID.randomUUID().toString();
            final String json = "{\"name\": \"" + name + "\", \"count\": 42}";

            final ConfigWithNoAnnotatedFields result = reader.readValue(json);

            assertThat(result, notNullValue());
            assertThat(result.getName(), equalTo(name));
            assertThat(result.getCount(), equalTo(42));
            verify(pluginFactory, never()).loadPlugin(any(), any(PluginSetting.class));
        }
    }

    @Nested
    class WithAnnotatedField {
        private ObjectReader reader;

        @BeforeEach
        void setUp() {
            reader = objectMapper.readerFor(ConfigWithAnnotatedField.class)
                    .withAttribute(NestedPluginDeserializer.PLUGIN_FACTORY_ATTRIBUTE_KEY, pluginFactory);
        }

        @Test
        void resolves_plugin_via_plugin_factory() throws JsonProcessingException {
            final String name = UUID.randomUUID().toString();
            final String pluginValue = UUID.randomUUID().toString();
            final TestPlugin mockPlugin = () -> pluginValue;
            when(pluginFactory.loadPlugin(eq(TestPlugin.class), any(PluginSetting.class)))
                    .thenReturn(mockPlugin);

            final String pluginName = UUID.randomUUID().toString();
            final String json = "{\"name\": \"" + name + "\", \"my_plugin\": {\"" + pluginName + "\": {\"option\": \"value\"}}}";

            final ConfigWithAnnotatedField result = reader.readValue(json);

            assertThat(result, notNullValue());
            assertThat(result.getName(), equalTo(name));
            assertThat(result.getMyPlugin(), sameInstance(mockPlugin));
            verify(pluginFactory).loadPlugin(eq(TestPlugin.class), any(PluginSetting.class));
        }

        @Test
        void leaves_field_null_when_not_present_in_input() throws JsonProcessingException {
            final String name = UUID.randomUUID().toString();
            final String json = "{\"name\": \"" + name + "\"}";

            final ConfigWithAnnotatedField result = reader.readValue(json);

            assertThat(result, notNullValue());
            assertThat(result.getName(), equalTo(name));
            assertThat(result.getMyPlugin(), nullValue());
            verify(pluginFactory, never()).loadPlugin(any(), any(PluginSetting.class));
        }
    }

    @Nested
    class WithMultipleAnnotatedFields {
        private ObjectReader reader;

        @BeforeEach
        void setUp() {
            reader = objectMapper.readerFor(ConfigWithMultipleFields.class)
                    .withAttribute(NestedPluginDeserializer.PLUGIN_FACTORY_ATTRIBUTE_KEY, pluginFactory);
        }

        @Test
        void resolves_all_annotated_fields_independently() throws JsonProcessingException {
            final String label = UUID.randomUUID().toString();
            final String description = UUID.randomUUID().toString();
            final String firstValue = UUID.randomUUID().toString();
            final String secondValue = UUID.randomUUID().toString();
            final TestPlugin firstMock = () -> firstValue;
            final TestPlugin secondMock = () -> secondValue;

            when(pluginFactory.loadPlugin(eq(TestPlugin.class), any(PluginSetting.class)))
                    .thenReturn(firstMock)
                    .thenReturn(secondMock);

            final String firstPluginName = UUID.randomUUID().toString();
            final String secondPluginName = UUID.randomUUID().toString();
            final String json = "{\"label\": \"" + label + "\", \"first_plugin\": {\"" + firstPluginName + "\": {}}, " +
                    "\"description\": \"" + description + "\", \"second_plugin\": {\"" + secondPluginName + "\": {\"key\": \"val\"}}}";

            final ConfigWithMultipleFields result = reader.readValue(json);

            assertThat(result, notNullValue());
            assertThat(result.getLabel(), equalTo(label));
            assertThat(result.getDescription(), equalTo(description));
            assertThat(result.getFirstPlugin(), sameInstance(firstMock));
            assertThat(result.getSecondPlugin(), sameInstance(secondMock));
        }

        @Test
        void resolves_only_present_fields_and_leaves_others_null() throws JsonProcessingException {
            final String label = UUID.randomUUID().toString();
            final String description = UUID.randomUUID().toString();
            final String pluginValue = UUID.randomUUID().toString();
            final TestPlugin mockPlugin = () -> pluginValue;
            when(pluginFactory.loadPlugin(eq(TestPlugin.class), any(PluginSetting.class)))
                    .thenReturn(mockPlugin);

            final String pluginName = UUID.randomUUID().toString();
            final String json = "{\"label\": \"" + label + "\", \"first_plugin\": {\"" + pluginName + "\": {}}, \"description\": \"" + description + "\"}";

            final ConfigWithMultipleFields result = reader.readValue(json);

            assertThat(result, notNullValue());
            assertThat(result.getLabel(), equalTo(label));
            assertThat(result.getDescription(), equalTo(description));
            assertThat(result.getFirstPlugin(), sameInstance(mockPlugin));
            assertThat(result.getSecondPlugin(), nullValue());
        }
    }

    @Nested
    class WithPluginModelField {
        private ObjectReader reader;

        @BeforeEach
        void setUp() {
            reader = objectMapper.readerFor(ConfigWithPluginModelField.class)
                    .withAttribute(NestedPluginDeserializer.PLUGIN_FACTORY_ATTRIBUTE_KEY, pluginFactory);
        }

        @Test
        void deserializes_as_plugin_model_without_invoking_plugin_factory() throws JsonProcessingException {
            final String name = UUID.randomUUID().toString();
            final String pluginName = UUID.randomUUID().toString();
            final String settingValue = UUID.randomUUID().toString();
            final String json = "{\"name\": \"" + name + "\", \"action\": {\"" + pluginName + "\": {\"key\": \"" + settingValue + "\"}}}";

            final ConfigWithPluginModelField result = reader.readValue(json);

            assertThat(result, notNullValue());
            assertThat(result.getName(), equalTo(name));
            assertThat(result.getAction(), notNullValue());
            assertThat(result.getAction().getPluginName(), equalTo(pluginName));
            assertThat(result.getAction().getPluginSettings().get("key"), equalTo(settingValue));
            verify(pluginFactory, never()).loadPlugin(any(), any(PluginSetting.class));
        }

        @Test
        void handles_null_plugin_model_field() throws JsonProcessingException {
            final String name = UUID.randomUUID().toString();
            final String json = "{\"name\": \"" + name + "\"}";

            final ConfigWithPluginModelField result = reader.readValue(json);

            assertThat(result, notNullValue());
            assertThat(result.getName(), equalTo(name));
            assertThat(result.getAction(), nullValue());
            verify(pluginFactory, never()).loadPlugin(any(), any(PluginSetting.class));
        }
    }

    @Nested
    class WithNestedPluginThatAlsoLoadsPlugin {
        private ObjectReader reader;

        @BeforeEach
        void setUp() {
            reader = objectMapper.readerFor(ConfigWithNestedPluginThatAlsoLoadsPlugin.class)
                    .withAttribute(NestedPluginDeserializer.PLUGIN_FACTORY_ATTRIBUTE_KEY, pluginFactory);
        }

        @Test
        void outer_plugin_loading_triggers_inner_plugin_loading() throws JsonProcessingException {
            final String name = UUID.randomUUID().toString();
            final String outerPluginName = UUID.randomUUID().toString();
            final String innerPluginName = UUID.randomUUID().toString();
            final String outerSetting = UUID.randomUUID().toString();
            final String innerValue = UUID.randomUUID().toString();
            final String outerValue = UUID.randomUUID().toString();

            final InnerPlugin innerPlugin = () -> innerValue;
            final TestPlugin outerPlugin = () -> outerValue;

            when(pluginFactory.loadPlugin(eq(TestPlugin.class), any(PluginSetting.class)))
                    .thenAnswer(invocation -> {
                        final PluginSetting outerPluginSetting = invocation.getArgument(1);
                        assertThat(outerPluginSetting.getName(), equalTo(outerPluginName));

                        final Map<String, Object> outerSettings = outerPluginSetting.getSettings();
                        assertThat(outerSettings.get("setting"), equalTo(outerSetting));

                        @SuppressWarnings("unchecked")
                        final Map<String, Object> innerPluginMap = (Map<String, Object>) outerSettings.get("inner_plugin");
                        assertThat(innerPluginMap, notNullValue());
                        assertThat(innerPluginMap.containsKey(innerPluginName), equalTo(true));

                        final PluginSetting innerPluginSetting = new PluginSetting(innerPluginName,
                                innerPluginMap.get(innerPluginName) != null
                                        ? (Map<String, Object>) innerPluginMap.get(innerPluginName)
                                        : Collections.emptyMap());

                        when(pluginFactory.loadPlugin(eq(InnerPlugin.class), any(PluginSetting.class)))
                                .thenReturn(innerPlugin);

                        final InnerPlugin resolvedInner = pluginFactory.loadPlugin(InnerPlugin.class, innerPluginSetting);
                        assertThat(resolvedInner, sameInstance(innerPlugin));

                        return outerPlugin;
                    });

            final String json = "{\"name\": \"" + name + "\", \"outer_plugin\": {\"" + outerPluginName +
                    "\": {\"setting\": \"" + outerSetting + "\", \"inner_plugin\": {\"" + innerPluginName + "\": {\"key\": \"val\"}}}}}";

            final ConfigWithNestedPluginThatAlsoLoadsPlugin result = reader.readValue(json);

            assertThat(result, notNullValue());
            assertThat(result.getName(), equalTo(name));
            assertThat(result.getOuterPlugin(), sameInstance(outerPlugin));
            verify(pluginFactory).loadPlugin(eq(TestPlugin.class), any(PluginSetting.class));
            verify(pluginFactory).loadPlugin(eq(InnerPlugin.class), any(PluginSetting.class));
        }
    }
}
