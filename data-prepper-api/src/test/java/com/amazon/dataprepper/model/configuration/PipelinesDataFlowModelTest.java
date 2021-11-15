package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;

class PipelinesDataFlowModelTest {

    private static final String RESOURCE_PATH = "/pipelines_data_flow_serialized.yaml";
    private ObjectMapper objectMapper;

    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper(YAMLFactory.builder()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .build());
    }

    @Test
    void testSerializing_PipelinesDataFlowModel_empty_Plugins() throws JsonProcessingException {
        String pipelineName = "test-pipeline";

        final PluginModel source = new PluginModel("testSource", null);
        final List<PluginModel> preppers = Collections.singletonList(new PluginModel("testPrepper", null));
        final List<PluginModel> sinks = Collections.singletonList(new PluginModel("testSink", null));
        final PipelineModel pipelineModel = new PipelineModel(source, preppers, sinks, null, null);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(Collections.singletonMap(pipelineName, pipelineModel));

        final String serializedString = objectMapper.writeValueAsString(pipelinesDataFlowModel);

        InputStream inputStream = this.getClass().getResourceAsStream(RESOURCE_PATH);

        final String expectedYaml = PluginModelTests.convertInputStreamToString(inputStream);

        assertThat(serializedString, notNullValue());
        assertThat(serializedString, equalTo(expectedYaml));
    }

    @Test
    void deserialize_PipelinesDataFlowModel() throws IOException {

        final InputStream inputStream = this.getClass().getResourceAsStream(RESOURCE_PATH);

        final PipelinesDataFlowModel actualModel = objectMapper.readValue(inputStream, PipelinesDataFlowModel.class);

        final String pipelineName = "test-pipeline";

        assertThat(actualModel, notNullValue());
        assertThat(actualModel.getPipelines(), notNullValue());
        assertThat(actualModel.getPipelines().size(), equalTo(1));
        assertThat(actualModel.getPipelines(), hasKey(pipelineName));

        final PipelineModel pipelineModel = actualModel.getPipelines().get(pipelineName);

        assertThat(pipelineModel, notNullValue());

        assertThat(pipelineModel.getSource(), notNullValue());
        assertThat(pipelineModel.getSource().getPluginName(), equalTo("testSource"));

        assertThat(pipelineModel.getPreppers(), notNullValue());
        assertThat(pipelineModel.getPreppers().size(), equalTo(1));
        assertThat(pipelineModel.getPreppers().get(0), notNullValue());
        assertThat(pipelineModel.getPreppers().get(0).getPluginName(), equalTo("testPrepper"));

        assertThat(pipelineModel.getSinks(), notNullValue());
        assertThat(pipelineModel.getSinks().size(), equalTo(1));
        assertThat(pipelineModel.getSinks().get(0), notNullValue());
        assertThat(pipelineModel.getSinks().get(0).getPluginName(), equalTo("testSink"));
    }
}
