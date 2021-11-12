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
        PipelineModel expectedPipelineModel = new PipelineModel(TestDataProvider.samplePluginModel(), null, null, 1, 3_000);

        assertThat(expectedPipelineModel.getSourcePluginModel().getPluginName(),
                equalTo(actualPipelineModel.getSourcePluginModel().getPluginName()));
        assertThat(expectedPipelineModel.getSourcePluginModel().getPluginSettings(),
                equalTo(actualPipelineModel.getSourcePluginModel().getPluginSettings()));
        assertThat(expectedPipelineModel.getReadBatchDelay(), equalTo(actualPipelineModel.getReadBatchDelay()));
        assertThat(expectedPipelineModel.getWorkers(), equalTo(actualPipelineModel.getWorkers()));
    }

    @Test
    void mapPipeline_with_multiple_source_plugins_should_throw_exception_Test() {
        LogstashConfiguration logstashConfiguration = TestDataProvider.sampleConfigurationWithMoreThanOnePlugin();

        Exception exception = assertThrows(LogstashMappingException.class, () ->
                logstashMapper.mapPipeline(logstashConfiguration));

        String expectedMessage = "More than 1 source plugins are not supported";
        String actualMessage = exception.getMessage();

        assertThat(expectedMessage, equalTo(actualMessage));
    }

}