/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.model.PipelineConfiguration;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class PipelineConfigurationValidatorIT {
    private static final String TEST_YAML_CONFIG =
            "entry-pipeline:\n" +
                    "  source:\n" +
                    "    random:\n" +
                    "  routes:\n" +
                    "    - route_one: \"/my_route == 10\"\n" +
                    "    - route_two: \"/my_route == 11\"\n" +
                    "  sink:\n" +
                    "    - pipeline:\n" +
                    "        name: \"sub-pipeline-1\"\n" +
                    "        routes:\n" +
                    "          - FIRST_ROUTE\n" +
                    "    - pipeline:\n" +
                    "        name: \"sub-pipeline-2\"\n" +
                    "        routes:\n" +
                    "          - SECOND_ROUTE\n";

    private static final String SIMPLE_PIPELINE_NO_ROUTES_CONFIG =
            "entry-pipeline:\n" +
                    "  source:\n" +
                    "    random:\n" +
                    "  sink:\n" +
                    "    - stdout:\n";

    private static final String ROUTES_DEFINED_NO_SINK_ROUTES_CONFIG =
            "entry-pipeline:\n" +
                    "  source:\n" +
                    "    random:\n" +
                    "  routes:\n" +
                    "    - route_one: \"/my_route == 10\"\n" +
                    "    - route_two: \"/my_route == 11\"\n" +
                    "  sink:\n" +
                    "    - stdout:\n";

    @Test
    public void test_with_invalid_sink_routes_in_yaml_should_throw() {
        Map<String, PipelineConfiguration> pipelineConfigMap = getPipelineConfigMap(TEST_YAML_CONFIG);

        InvalidPipelineConfigurationException exception = assertThrows(InvalidPipelineConfigurationException.class, () ->
                PipelineConfigurationValidator.validateAndGetPipelineNames(pipelineConfigMap));

        String expectedErrorMessage = "The following routes do not exist in pipeline \"entry-pipeline\": " +
                "[FIRST_ROUTE]. Configured routes include [route_one, route_two]";

        assertThat(exception.getMessage(), equalTo(expectedErrorMessage));
    }

    @Test
    public void test_simple_pipeline_with_no_routes_succeeds() {
        Map<String, PipelineConfiguration> pipelineConfigMap = getPipelineConfigMap(SIMPLE_PIPELINE_NO_ROUTES_CONFIG);

        List<String> pipelineNames = assertDoesNotThrow(() ->
                PipelineConfigurationValidator.validateAndGetPipelineNames(pipelineConfigMap));

        assertThat(pipelineNames, hasSize(1));
        assertThat(pipelineNames.get(0), equalTo("entry-pipeline"));
    }

    @Test
    public void test_routes_defined_but_no_routes_on_sinks_succeeds() {
        Map<String, PipelineConfiguration> pipelineConfigMap = getPipelineConfigMap(ROUTES_DEFINED_NO_SINK_ROUTES_CONFIG);

        List<String> pipelineNames = assertDoesNotThrow(() ->
                PipelineConfigurationValidator.validateAndGetPipelineNames(pipelineConfigMap));

        assertThat(pipelineNames, hasSize(1));
        assertThat(pipelineNames.get(0), equalTo("entry-pipeline"));
    }

    /**
     * Helper method to create a pipeline configuration map from a YAML string
     */
    private Map<String, PipelineConfiguration> getPipelineConfigMap(final String yamlConfig) {
        PipelineConfigurationReader configReader = mock(PipelineConfigurationReader.class);
        when(configReader.getPipelineConfigurationInputStreams())
                .thenReturn(List.of(new ByteArrayInputStream(yamlConfig.getBytes())));

        PipelinesDataflowModelParser modelParser = new PipelinesDataflowModelParser(configReader);
        PipelinesDataFlowModel dataFlowModel = modelParser.parseConfiguration();

        return dataFlowModel.getPipelines().entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> new PipelineConfiguration(entry.getValue())
                ));
    }
}