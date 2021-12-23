/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.logstash.mapping;

import com.amazon.dataprepper.model.configuration.PipelineModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.logstash.exception.LogstashMappingException;
import org.opensearch.dataprepper.logstash.model.LogstashConfiguration;
import org.opensearch.dataprepper.logstash.model.LogstashPluginType;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
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

        PipelineModel actualPipelineModel =  logstashMapper.mapPipeline(logstashConfiguration);
        PipelineModel expectedPipelineModel = new PipelineModel(TestDataProvider.samplePluginModel(),
                null, null, null, null);

        assertThat(actualPipelineModel.getSource().getPluginName(),
                equalTo(expectedPipelineModel.getSource().getPluginName()));
        assertThat(actualPipelineModel.getSource().getPluginSettings(),
                equalTo(expectedPipelineModel.getSource().getPluginSettings()));
        assertThat(actualPipelineModel.getReadBatchDelay(), equalTo(expectedPipelineModel.getReadBatchDelay()));
        assertThat(actualPipelineModel.getWorkers(), equalTo(expectedPipelineModel.getWorkers()));
    }

    @Test
    void mapPipeline_with_multiple_source_plugins_should_throw_exception_Test() {
        LogstashConfiguration logstashConfiguration = TestDataProvider.sampleConfigurationWithMoreThanOnePlugin();

        Exception exception = assertThrows(LogstashMappingException.class, () ->
                logstashMapper.mapPipeline(logstashConfiguration));

        String expectedMessage = "More than 1 source plugins are not supported";
        String actualMessage = exception.getMessage();

        assertThat(actualMessage, equalTo(expectedMessage));
    }

    @Test
    void mapPipeline_with_no_plugins_should_return_pipeline_model_without_plugins_Test() {
        LogstashConfiguration logstashConfiguration = mock(LogstashConfiguration.class);
        when(logstashConfiguration.getPluginSection(LogstashPluginType.INPUT))
                .thenReturn(Collections.emptyList());
        when(logstashConfiguration.getPluginSection(LogstashPluginType.FILTER))
                .thenReturn(Collections.emptyList());
        when(logstashConfiguration.getPluginSection(LogstashPluginType.OUTPUT))
                .thenReturn(Collections.emptyList());

        PipelineModel actualPipelineModel =  logstashMapper.mapPipeline(logstashConfiguration);

        assertThat(actualPipelineModel.getSource(), equalTo(null));
        assertThat(actualPipelineModel.getProcessors(), equalTo(Collections.emptyList()));
        assertThat(actualPipelineModel.getSinks(), equalTo(Collections.emptyList()));
        assertThat(actualPipelineModel.getReadBatchDelay(), equalTo(null));
        assertThat(actualPipelineModel.getWorkers(), equalTo(null));
    }

}