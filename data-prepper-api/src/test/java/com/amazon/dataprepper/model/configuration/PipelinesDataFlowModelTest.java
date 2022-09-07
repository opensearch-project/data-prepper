/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

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
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;

class PipelinesDataFlowModelTest {

    private static final String RESOURCE_PATH = "/pipelines_data_flow_serialized.yaml";
    private static final String RESOURCE_PATH_WITH_ROUTE = "/pipelines_data_flow_route.yaml";
    private ObjectMapper objectMapper;

    @BeforeEach
    void setup() {
        objectMapper = new ObjectMapper(YAMLFactory.builder()
                .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
                .enable(YAMLGenerator.Feature.USE_PLATFORM_LINE_BREAKS)
                .build());
    }

    @Test
    void testSerializing_PipelinesDataFlowModel_empty_Plugins_with_nonEmpty_delay_and_workers() throws JsonProcessingException {
        String pipelineName = "test-pipeline";

        final PluginModel source = new PluginModel("testSource", (Map<String, Object>) null);
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", (Map<String, Object>) null));
        final List<PluginModel> sinks = Collections.singletonList(new PluginModel("testSink", (Map<String, Object>) null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(Collections.singletonMap(pipelineName, pipelineModel));

        final String serializedString = objectMapper.writeValueAsString(pipelinesDataFlowModel);

        InputStream inputStream = this.getClass().getResourceAsStream(RESOURCE_PATH);

        final String expectedYaml = PluginModelTests.convertInputStreamToString(inputStream);

        assertThat(serializedString, notNullValue());
        assertThat(serializedString, equalTo(expectedYaml));
    }

    @Test
    void testSerializing_PipelinesDataFlowModel_empty_Plugins_with_nonEmpty_delay_and_workers_and_route() throws JsonProcessingException {
        String pipelineName = "test-pipeline";

        final PluginModel source = new PluginModel("testSource", null);
        final List<PluginModel> preppers = Collections.singletonList(new PluginModel("testPrepper", null));
        final List<PluginModel> sinks = Collections.singletonList(new PluginModel("testSink", null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, preppers, Collections.singletonList(new ConditionalRoute("my-route", "/a==b")), sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(Collections.singletonMap(pipelineName, pipelineModel));

        final String serializedString = objectMapper.writeValueAsString(pipelinesDataFlowModel);

        InputStream inputStream = this.getClass().getResourceAsStream(RESOURCE_PATH_WITH_ROUTE);

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

        assertThat(pipelineModel.getProcessors(), notNullValue());
        assertThat(pipelineModel.getProcessors().size(), equalTo(1));
        assertThat(pipelineModel.getProcessors().get(0), notNullValue());
        assertThat(pipelineModel.getProcessors().get(0).getPluginName(), equalTo("testProcessor"));

        assertThat(pipelineModel.getSinks(), notNullValue());
        assertThat(pipelineModel.getSinks().size(), equalTo(1));
        assertThat(pipelineModel.getSinks().get(0), notNullValue());
        assertThat(pipelineModel.getSinks().get(0).getPluginName(), equalTo("testSink"));

        assertThat(pipelineModel.getRoutes(), notNullValue());
        assertThat(pipelineModel.getRoutes().size(), equalTo(0));
    }

    @Test
    void deserialize_PipelinesDataFlowModel_with_route() throws IOException {

        final InputStream inputStream = this.getClass().getResourceAsStream(RESOURCE_PATH_WITH_ROUTE);

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

        assertThat(pipelineModel.getRoutes(), notNullValue());
        assertThat(pipelineModel.getRoutes().size(), equalTo(1));
        assertThat(pipelineModel.getRoutes().get(0), notNullValue());
        assertThat(pipelineModel.getRoutes().get(0).getName(), equalTo("my-route"));
        assertThat(pipelineModel.getRoutes().get(0).getCondition(), equalTo("/a==b"));

    }
}
