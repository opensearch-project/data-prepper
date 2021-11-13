package com.amazon.dataprepper.model.configuration;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

class PipelinesDataFlowModelTest {

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

        InputStream inputStream = this.getClass().getResourceAsStream("/pipelines_data_flow_serialized.yaml");

        final String expectedYaml = PluginModelTests.convertInputStreamToString(inputStream);

        assertThat(serializedString, notNullValue());
        assertThat(serializedString, equalTo(expectedYaml));
    }
}
