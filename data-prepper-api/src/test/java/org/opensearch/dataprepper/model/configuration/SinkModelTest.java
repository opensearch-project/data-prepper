/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.configuration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SinkModelTest {
    private ObjectMapper objectMapper;

    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper(YAMLFactory.builder()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS)
                .build());
    }

    @Test
    void deserialize_with_Sink() throws IOException {
        final InputStream inputStream = this.getClass().getResourceAsStream("sink_plugin.yaml");

        final SinkModel sinkModel = objectMapper.readValue(inputStream, SinkModel.class);

        assertThat(sinkModel, notNullValue());

        assertAll(
                () -> assertThat(sinkModel.getPluginName(), equalTo("customSinkPlugin")),
                () -> assertThat(sinkModel.getPluginSettings(), notNullValue()),
                () -> assertThat(sinkModel.getRoutes(), notNullValue())
        );
        assertAll(
                () -> assertThat(sinkModel.getPluginSettings().size(), equalTo(2)),
                () -> assertThat(sinkModel.getPluginSettings(), hasKey("key1")),
                () -> assertThat(sinkModel.getPluginSettings(), hasKey("key2"))
        );

        assertAll(
                () -> assertThat(sinkModel.getRoutes().size(), equalTo(2)),
                () -> assertThat(sinkModel.getRoutes(), hasItem("routeA")),
                () -> assertThat(sinkModel.getRoutes(), hasItem("routeB"))
        );
    }

    @Test
    void serialize_into_known_SinkModel() throws IOException {
        final Map<String, Object> pluginSettings = new LinkedHashMap<>();
        pluginSettings.put("key1", "value1");
        pluginSettings.put("key2", "value2");
        final String tagsTargetKey = "tags";
        final SinkModel sinkModel = new SinkModel("customSinkPlugin", Arrays.asList("routeA", "routeB"), tagsTargetKey, Collections.emptyList(), Collections.emptyList(), pluginSettings);

        final String actualJson = objectMapper.writeValueAsString(sinkModel);

        final String expectedJson = createStringFromInputStream(this.getClass().getResourceAsStream("sink_plugin.yaml"));

        assertThat("---\n" + actualJson, equalTo(expectedJson));
        assertThat(sinkModel.getTagsTargetKey(), equalTo(tagsTargetKey));

    }


    @Test
    void deserialize_with_any_pluginModel() throws IOException {
        final InputStream inputStream = this.getClass().getResourceAsStream("/serialized_with_plugin_settings.yaml");

        final SinkModel sinkModel = objectMapper.readValue(inputStream, SinkModel.class);

        assertThat(sinkModel, notNullValue());
        assertAll(
                () -> assertThat(sinkModel.getPluginName(), equalTo("customPlugin")),
                () -> assertThat(sinkModel.getPluginSettings(), notNullValue()),
                () -> assertThat(sinkModel.getRoutes(), notNullValue()),
                () -> assertThat(sinkModel.getTagsTargetKey(), nullValue())
        );
        assertAll(
                () -> assertThat(sinkModel.getPluginSettings().size(), equalTo(3)),
                () -> assertThat(sinkModel.getPluginSettings(), hasKey("key1")),
                () -> assertThat(sinkModel.getPluginSettings(), hasKey("key2")),
                () -> assertThat(sinkModel.getPluginSettings(), hasKey("key3"))
        );
        assertThat(sinkModel.getRoutes().size(), equalTo(0));
    }

    @ParameterizedTest
    @ValueSource(strings = {"plugin_model_empty.yaml", "plugin_model_not_present.yaml", "plugin_model_null.yaml"})
    final void deserialize_with_empty_inner(final String resourceName) throws IOException {
        final InputStream inputStream = PluginModelTests.class.getResourceAsStream(resourceName);

        final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

        final SinkModel sinkModel = mapper.readValue(inputStream, SinkModel.class);
        assertThat(sinkModel.getPluginName(), equalTo("customPlugin"));
        assertThat(sinkModel.getPluginSettings(), notNullValue());
        assertThat(sinkModel.getPluginSettings().size(), equalTo(0));
    }

    @Test
    void serialize_with_just_pluginModel() throws IOException {
        final Map<String, Object> pluginSettings = new LinkedHashMap<>();
        pluginSettings.put("key1", "value1");
        pluginSettings.put("key2", "value2");
        pluginSettings.put("key3", "value3");
        final SinkModel sinkModel = new SinkModel("customPlugin", null, null, Collections.emptyList(), Collections.emptyList(), pluginSettings);

        final String actualJson = objectMapper.writeValueAsString(sinkModel);

        final String expectedJson = createStringFromInputStream(this.getClass().getResourceAsStream("/serialized_with_plugin_settings.yaml"));

        assertThat("---\n" + actualJson, equalTo(expectedJson));
    }

    @Test
    void sinkModel_with_include_keys() {
        final Map<String, Object> pluginSettings = new LinkedHashMap<>();
        final SinkModel sinkModel = new SinkModel("customSinkPlugin", Arrays.asList("routeA", "routeB"), null, Arrays.asList("bcd", "abc", "efg"), null, pluginSettings);

        assertThat(sinkModel.getExcludeKeys(), equalTo(new ArrayList<String>()));
        assertThat(sinkModel.getIncludeKeys(), equalTo(Arrays.asList("bcd", "abc", "efg")));

    }

    @Test
    void sinkModel_with_invalid_include_keys() {
        final Map<String, Object> pluginSettings = new LinkedHashMap<>();
        assertThrows(InvalidPluginConfigurationException.class, () -> new SinkModel("customSinkPlugin", Arrays.asList("routeA", "routeB"), null, List.of("/bcd"), List.of(), pluginSettings));
    }

    @Test
    void sinkModel_with_exclude_keys() {
        final Map<String, Object> pluginSettings = new LinkedHashMap<>();
        final SinkModel sinkModel = new SinkModel("customSinkPlugin", Arrays.asList("routeA", "routeB"), null, List.of(), Arrays.asList("abc", "bcd", "efg"), pluginSettings);

        assertThat(sinkModel.getIncludeKeys(), equalTo(new ArrayList<String>()));
        assertThat(sinkModel.getExcludeKeys(), equalTo(Arrays.asList("abc", "bcd", "efg")));

    }

    @Test
    void sinkModel_with_invalid_exclude_keys() {
        final Map<String, Object> pluginSettings = new LinkedHashMap<>();
        assertThrows(InvalidPluginConfigurationException.class, () -> new SinkModel("customSinkPlugin", Arrays.asList("routeA", "routeB"), null, List.of(), List.of("/bcd"), pluginSettings));
    }



    @Test
    void sinkModel_with_both_include_and_exclude_keys() {
        final Map<String, Object> pluginSettings = new LinkedHashMap<>();
        assertThrows(InvalidPluginConfigurationException.class, () -> new SinkModel("customSinkPlugin", Arrays.asList("routeA", "routeB"), null, List.of("abc"), List.of("bcd"), pluginSettings));
    }

    @Nested
    class BuilderTest {
        private PluginModel pluginModel;
        private String pluginName;
        private Map<String, Object> pluginSettings;

        @BeforeEach
        void setUp() {
            pluginName = UUID.randomUUID().toString();
            pluginSettings = Map.of(UUID.randomUUID().toString(), UUID.randomUUID().toString());
            pluginModel = mock(PluginModel.class);
            when(pluginModel.getPluginName()).thenReturn(pluginName);
            when(pluginModel.getPluginSettings()).thenReturn(pluginSettings);
        }

        @Test
        void build_with_only_PluginModel_should_return_expected_SinkModel() {
            final SinkModel actualSinkModel = SinkModel.builder(pluginModel).build();

            assertThat(actualSinkModel, notNullValue());
            assertThat(actualSinkModel.getPluginName(), equalTo(pluginName));
            assertThat(actualSinkModel.getPluginSettings(), equalTo(pluginSettings));
            assertThat(actualSinkModel.getRoutes(), notNullValue());
            assertThat(actualSinkModel.getRoutes(), empty());
            assertThat(actualSinkModel.getIncludeKeys(), notNullValue());
            assertThat(actualSinkModel.getIncludeKeys(), empty());
            assertThat(actualSinkModel.getExcludeKeys(), notNullValue());
            assertThat(actualSinkModel.getExcludeKeys(), empty());
            assertThat(actualSinkModel.getTagsTargetKey(), nullValue());
            assertThat(actualSinkModel.getTagsTargetKey(), nullValue());

        }
    }

    private static String createStringFromInputStream(final InputStream inputStream) throws IOException {
        return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
    }
}