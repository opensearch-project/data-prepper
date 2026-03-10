/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.model.configuration;

import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * End-to-end tests for sample pipeline YAML configurations, verifying that
 * valid plugin config formats are accepted and invalid ones are rejected.
 *
 * Valid formats:  null, empty value (no value after colon), {}
 * Invalid format: "" (empty string)
 */
class SamplePipelineConfigurationTest {

    private static final String PIPELINE_NAME = "test-pipeline";

    private ObjectMapper objectMapper;

    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper(new YAMLFactory());
    }

    @ParameterizedTest(name = "pipeline with plugin config [{0}] should deserialize successfully")
    @ValueSource(strings = {
        "sample_pipelines/sample_pipeline_plugin_null.yaml",
        "sample_pipelines/sample_pipeline_plugin_empty_value.yaml",
        "sample_pipelines/sample_pipeline_plugin_empty_object.yaml",
        "sample_pipelines/sample_pipeline_plugin_with_settings.yaml"
    })
    void deserialize_validPipeline_succeeds(final String resourcePath) throws IOException {
        final InputStream inputStream = getClass().getResourceAsStream(resourcePath);

        final PipelinesDataFlowModel model = objectMapper.readValue(inputStream, PipelinesDataFlowModel.class);

        assertThat(model, notNullValue());
        assertThat(model.getPipelines(), notNullValue());
        assertThat(model.getPipelines().containsKey(PIPELINE_NAME), equalTo(true));

        final PipelineModel pipeline = model.getPipelines().get(PIPELINE_NAME);
        assertThat(pipeline.getSource(), notNullValue());
        assertThat(pipeline.getSource().getPluginName(), notNullValue());
        assertThat(pipeline.getSinks(), notNullValue());
        assertThat(pipeline.getSinks().size(), equalTo(1));
    }

    @Test
    void deserialize_pipeline_withPluginSettings_hasCorrectValues() throws IOException {
        final InputStream inputStream = getClass().getResourceAsStream("sample_pipelines/sample_pipeline_plugin_with_settings.yaml");

        final PipelinesDataFlowModel model = objectMapper.readValue(inputStream, PipelinesDataFlowModel.class);
        final PipelineModel pipeline = model.getPipelines().get(PIPELINE_NAME);

        // source: http with host/port settings
        assertThat(pipeline.getSource().getPluginName(), equalTo("http"));
        assertThat(pipeline.getSource().getPluginSettings().get("host"), equalTo("0.0.0.0"));
        assertThat(pipeline.getSource().getPluginSettings().get("port"), equalTo(2021));

        // processor: grok
        assertThat(pipeline.getProcessors().get(0).getPluginName(), equalTo("grok"));

        // sink: opensearch with hosts/credentials
        assertThat(pipeline.getSinks().get(0).getPluginName(), equalTo("opensearch"));
        assertThat(pipeline.getSinks().get(0).getPluginSettings().containsKey("hosts"), equalTo(true));
    }

    @ParameterizedTest(name = "pipeline with null/empty plugin [{0}] should have empty plugin settings")
    @ValueSource(strings = {
        "sample_pipelines/sample_pipeline_plugin_null.yaml",
        "sample_pipelines/sample_pipeline_plugin_empty_value.yaml",
        "sample_pipelines/sample_pipeline_plugin_empty_object.yaml"
    })
    void deserialize_pipeline_withEmptyPluginConfig_hasEmptySettings(final String resourcePath) throws IOException {
        final InputStream inputStream = getClass().getResourceAsStream(resourcePath);

        final PipelinesDataFlowModel model = objectMapper.readValue(inputStream, PipelinesDataFlowModel.class);
        final PipelineModel pipeline = model.getPipelines().get(PIPELINE_NAME);

        final Map<String, Object> sourceSettings = pipeline.getSource().getPluginSettings();
        assertThat(sourceSettings == null || sourceSettings.isEmpty(), equalTo(true));
        final Map<String, Object> processorSettings = pipeline.getProcessors().get(0).getPluginSettings();
        assertThat(processorSettings == null || processorSettings.isEmpty(), equalTo(true));
        final Map<String, Object> sinkSettings = pipeline.getSinks().get(0).getPluginSettings();
        assertThat(sinkSettings == null || sinkSettings.isEmpty(), equalTo(true));
    }

    @Test
    void deserialize_pipeline_withEmptyStringPluginConfig_throwsException() {
        final InputStream inputStream = getClass().getResourceAsStream("sample_pipelines/sample_pipeline_plugin_empty_string.yaml");

        final JsonMappingException exception = assertThrows(
            JsonMappingException.class,
            () -> objectMapper.readValue(inputStream, PipelinesDataFlowModel.class)
        );
        assertThat(exception.getMessage(), containsString("Empty string is not allowed"));
    }
}
