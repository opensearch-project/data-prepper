/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import org.opensearch.dataprepper.model.configuration.PipelineModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.logstash.exception.LogstashMappingException;
import org.opensearch.dataprepper.logstash.model.LogstashConfiguration;
import org.opensearch.dataprepper.logstash.model.LogstashPluginType;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class LogstashMapperTest {

    private LogstashMapper logstashMapper;

    @BeforeEach
    void createObjectUnderTest() {
        logstashMapper = new LogstashMapper();
    }

    @Test
    void mapPipeline_returns_pipeline_model() {
        LogstashConfiguration logstashConfiguration = mock(LogstashConfiguration.class);
        when(logstashConfiguration.getPluginSection(LogstashPluginType.INPUT))
                .thenReturn(Collections.singletonList(TestDataProvider.pluginData()));
        when(logstashConfiguration.getPluginSection(LogstashPluginType.OUTPUT))
                .thenReturn(Collections.singletonList(TestDataProvider.pluginData()));

        PipelineModel actualPipelineModel =  logstashMapper.mapPipeline(logstashConfiguration);
        PipelineModel expectedPipelineModel = new PipelineModel(TestDataProvider.samplePluginModel(),
                null, null, null, Collections.singletonList(TestDataProvider.sampleSinkModel()), null, null);

        assertThat(actualPipelineModel.getSource().getPluginName(),
                equalTo(expectedPipelineModel.getSource().getPluginName()));
        assertThat(actualPipelineModel.getSource().getPluginSettings(),
                equalTo(expectedPipelineModel.getSource().getPluginSettings()));
        assertThat(actualPipelineModel.getBuffer(), equalTo(expectedPipelineModel.getBuffer()));
        assertThat(actualPipelineModel.getSinks(), notNullValue());
        assertThat(actualPipelineModel.getSinks().size(), equalTo(1));
        assertThat(actualPipelineModel.getSinks().get(0).getPluginName(), equalTo(expectedPipelineModel.getSinks().get(0).getPluginName()));
        assertThat(actualPipelineModel.getReadBatchDelay(), equalTo(expectedPipelineModel.getReadBatchDelay()));
        assertThat(actualPipelineModel.getWorkers(), equalTo(expectedPipelineModel.getWorkers()));
    }

    @Test
    void mapPipeline_with_no_source_plugins_should_throw_exception() {
        final LogstashConfiguration logstashConfiguration = TestDataProvider.sampleConfigurationWithEmptyInputPlugins();

        final Exception exception = assertThrows(LogstashMappingException.class, () ->
                logstashMapper.mapPipeline(logstashConfiguration));

        final String expectedMessage = "Only logstash configurations with exactly 1 input plugin are supported";
        final String actualMessage = exception.getMessage();

        assertThat(actualMessage, equalTo(expectedMessage));
    }

    @Test
    void mapPipeline_with_multiple_source_plugins_should_throw_exception_Test() {
        LogstashConfiguration logstashConfiguration = TestDataProvider.sampleConfigurationWithMoreThanOnePlugin();

        Exception exception = assertThrows(LogstashMappingException.class, () ->
                logstashMapper.mapPipeline(logstashConfiguration));

        String expectedMessage = "Only logstash configurations with exactly 1 input plugin are supported";
        String actualMessage = exception.getMessage();

        assertThat(actualMessage, equalTo(expectedMessage));
    }

    @Test
    void mapPipeline_with_no_sink_plugins_should_throw_Exception() {
        final LogstashConfiguration logstashConfiguration = mock(LogstashConfiguration.class);

        when(logstashConfiguration.getPluginSection(LogstashPluginType.INPUT))
                .thenReturn(Collections.singletonList(TestDataProvider.pluginData()));
        when(logstashConfiguration.getPluginSection(LogstashPluginType.OUTPUT))
                .thenReturn(Collections.emptyList());

        final Exception exception = assertThrows(LogstashMappingException.class, () ->
                logstashMapper.mapPipeline(logstashConfiguration));

        final String expectedMessage = "At least one logstash output plugin is required";
        final String actualMessage = exception.getMessage();

        assertThat(actualMessage, equalTo(expectedMessage));
    }
}