/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.configuration;

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
import java.util.Optional;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.jupiter.api.Assertions.assertAll;

class PipelinesDataFlowModelTest {

    private static final String RESOURCE_PATH = "/pipelines_data_flow_serialized.yaml";
    private static final String RESOURCE_PATH_WITH_ROUTE = "/pipelines_data_flow_route.yaml";
    private static final String RESOURCE_PATH_WITH_ROUTES = "/pipelines_data_flow_routes.yaml";
    private static final String RESOURCE_PATH_WITH_SHORT_HAND_VERSION = "/pipeline_with_short_hand_version.yaml";
    private static final String RESOURCE_PATH_WITH_VERSION = "/pipeline_with_version.yaml";
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
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(Collections.singletonMap(pipelineName, pipelineModel));

        final String serializedString = objectMapper.writeValueAsString(pipelinesDataFlowModel);

        InputStream inputStream = this.getClass().getResourceAsStream(RESOURCE_PATH);

        final String expectedYaml = PluginModelTests.convertInputStreamToString(inputStream);

        assertThat(serializedString, notNullValue());
        assertThat(serializedString, equalTo(expectedYaml));
    }

    @Test
    void testSerializing_PipelinesDataFlowModel_with_Version() throws JsonProcessingException {
        String pipelineName = "test-pipeline";

        final DataPrepperVersion version = DataPrepperVersion.parse("2.0");
        final PluginModel source = new PluginModel("testSource", (Map<String, Object>) null);
        final List<PluginModel> processors = Collections.singletonList(new PluginModel("testProcessor", (Map<String, Object>) null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.emptyList(), null));
        final PipelineModel pipelineModel = new PipelineModel(source, null, processors, null, sinks, 8, 50);

        final PipelinesDataFlowModel pipelinesDataFlowModel = new PipelinesDataFlowModel(version, Collections.singletonMap(pipelineName, pipelineModel));

        final String serializedString = objectMapper.writeValueAsString(pipelinesDataFlowModel);

        InputStream inputStream = this.getClass().getResourceAsStream(RESOURCE_PATH_WITH_VERSION);

        final String expectedYaml = PluginModelTests.convertInputStreamToString(inputStream);

        assertThat(serializedString, notNullValue());
        assertThat(serializedString, equalTo(expectedYaml));
    }

    @Test
    void testSerializing_PipelinesDataFlowModel_empty_Plugins_with_nonEmpty_delay_and_workers_and_route() throws JsonProcessingException {
        String pipelineName = "test-pipeline";

        final PluginModel source = new PluginModel("testSource", (Map<String, Object>) null);
        final List<PluginModel> preppers = Collections.singletonList(new PluginModel("testPrepper", (Map<String, Object>) null));
        final List<SinkModel> sinks = Collections.singletonList(new SinkModel("testSink", Collections.singletonList("my-route"), null));
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

        assertThat(pipelineModel.getProcessors(), notNullValue());
        assertThat(pipelineModel.getProcessors().size(), equalTo(1));
        assertThat(pipelineModel.getProcessors().get(0), notNullValue());
        assertThat(pipelineModel.getProcessors().get(0).getPluginName(), equalTo("testPrepper"));

        assertThat(pipelineModel.getSinks(), notNullValue());
        assertThat(pipelineModel.getSinks().size(), equalTo(1));
        assertThat(pipelineModel.getSinks().get(0), notNullValue());
        assertThat(pipelineModel.getSinks().get(0).getPluginName(), equalTo("testSink"));
        assertThat(pipelineModel.getSinks().get(0).getRoutes(), notNullValue());
        assertAll(
                () -> assertThat(pipelineModel.getSinks().get(0).getRoutes().size(), equalTo(1)),
                () -> assertThat(pipelineModel.getSinks().get(0).getRoutes(), hasItem("my-route"))
        );

        assertThat(pipelineModel.getRoutes(), notNullValue());
        assertThat(pipelineModel.getRoutes().size(), equalTo(1));
        assertThat(pipelineModel.getRoutes().get(0), notNullValue());
        assertThat(pipelineModel.getRoutes().get(0).getName(), equalTo("my-route"));
        assertThat(pipelineModel.getRoutes().get(0).getCondition(), equalTo("/a==b"));
    }

    @Test
    void deserialize_PipelinesDataFlowModel_with_routes() throws IOException {

        final InputStream inputStream = this.getClass().getResourceAsStream(RESOURCE_PATH_WITH_ROUTES);

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
        assertThat(pipelineModel.getProcessors().get(0).getPluginName(), equalTo("testPrepper"));

        assertThat(pipelineModel.getSinks(), notNullValue());
        assertThat(pipelineModel.getSinks().size(), equalTo(1));
        assertThat(pipelineModel.getSinks().get(0), notNullValue());
        assertThat(pipelineModel.getSinks().get(0).getPluginName(), equalTo("testSink"));
        assertThat(pipelineModel.getSinks().get(0).getRoutes(), notNullValue());
        assertAll(
                () -> assertThat(pipelineModel.getSinks().get(0).getRoutes().size(), equalTo(1)),
                () -> assertThat(pipelineModel.getSinks().get(0).getRoutes(), hasItem("my-route"))
        );

        assertThat(pipelineModel.getRoutes(), notNullValue());
        assertThat(pipelineModel.getRoutes().size(), equalTo(1));
        assertThat(pipelineModel.getRoutes().get(0), notNullValue());
        assertThat(pipelineModel.getRoutes().get(0).getName(), equalTo("my-route"));
        assertThat(pipelineModel.getRoutes().get(0).getCondition(), equalTo("/a==b"));
    }

    @Test
    void deserialize_PipelinesDataFlowModel_with_shorthand_version() throws IOException {

        final InputStream inputStream = this.getClass().getResourceAsStream(RESOURCE_PATH_WITH_SHORT_HAND_VERSION);

        final PipelinesDataFlowModel actualModel = objectMapper.readValue(inputStream, PipelinesDataFlowModel.class);

        final String pipelineName = "test-pipeline";

        assertThat(actualModel, notNullValue());
        assertThat(actualModel.getPipelines(), notNullValue());
        assertThat(actualModel.getPipelines().size(), equalTo(1));
        assertThat(actualModel.getPipelines(), hasKey(pipelineName));

        final DataPrepperVersion version = actualModel.getDataPrepperVersion();
        assertThat(version.getMajorVersion(), is(equalTo(2)));
        assertThat(version.getMinorVersion(), is(equalTo(Optional.empty())));
    }
}
