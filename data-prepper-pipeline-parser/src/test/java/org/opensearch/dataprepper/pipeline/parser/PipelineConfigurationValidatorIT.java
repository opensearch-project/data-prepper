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

    @Test
    public void test_with_invalid_sink_routes_in_yaml_should_throw() {
        PipelineConfigurationReader configReader = mock(PipelineConfigurationReader.class);
        when(configReader.getPipelineConfigurationInputStreams())
                .thenReturn(List.of(new ByteArrayInputStream(TEST_YAML_CONFIG.getBytes())));

        PipelinesDataflowModelParser modelParser = new PipelinesDataflowModelParser(configReader);
        PipelinesDataFlowModel dataFlowModel = modelParser.parseConfiguration();

        Map<String, PipelineConfiguration> pipelineConfigMap = dataFlowModel.getPipelines().entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> new PipelineConfiguration(entry.getValue())
                ));

        assertThrows(RuntimeException.class, () ->
                PipelineConfigurationValidator.validateAndGetPipelineNames(pipelineConfigMap));
    }
}