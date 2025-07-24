/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.pipeline.server;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.opensearch.dataprepper.core.exception.DynamicPipelineConfigUpdateException;
import org.opensearch.dataprepper.model.configuration.PipelinesDataFlowModel;
import org.opensearch.dataprepper.pipeline.parser.PipelineConfigurationFileReader;
import org.opensearch.dataprepper.pipeline.parser.PipelinesDataflowModelParser;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class DynamicPipelineUpdateUtilTest {

    private static final String SCENARIOS_BASE_PATH = "src/test/resources/dynamic-pipeline-config-update";
    private static final String SOURCE_CONFIGURATION_ERROR = "Source configuration cannot be modified in pipeline: log-pipeline";
    private static final String SINKS_CONFIGURATION_ERROR = "Sinks configuration cannot be modified in pipeline: log-pipeline";
    private static final String SINGLE_THREADED_PROCESSOR_ERROR = "Cannot add new single-threaded processor: aggregate";

    @ParameterizedTest
    @ValueSource(strings = {
            SCENARIOS_BASE_PATH + "/same-processor-source-sink",
            SCENARIOS_BASE_PATH + "/same-processor-different-settings",
            SCENARIOS_BASE_PATH + "/multi-pipelines"
    })
    public void test_valid_pipeline_updates_are_feasible(String scenarioFolder) {
        PipelinesDataFlowModel currentPipelinesDataFlowModel =
                new PipelinesDataflowModelParser(
                        new PipelineConfigurationFileReader(scenarioFolder + "/current")
                ).parseConfiguration();

        PipelinesDataFlowModel targetPipelinesDataFlowModel =
                new PipelinesDataflowModelParser(
                        new PipelineConfigurationFileReader(scenarioFolder + "/target")
                ).parseConfiguration();
        boolean dynamicUpdateFeasible =
                DynamicPipelineUpdateUtil.isDynamicUpdateFeasible(currentPipelinesDataFlowModel, targetPipelinesDataFlowModel);
        assertTrue(dynamicUpdateFeasible);
    }

    @ParameterizedTest
    @ValueSource(strings = {
            
            SCENARIOS_BASE_PATH + "/invalid-multi-pipelines"
    })
    public void test_invalid_pipeline_updates_throw_exceptions(String scenarioFolder) {
        PipelinesDataFlowModel currentPipelinesDataFlowModel =
                new PipelinesDataflowModelParser(
                        new PipelineConfigurationFileReader(scenarioFolder + "/current")
                ).parseConfiguration();

        PipelinesDataFlowModel targetPipelinesDataFlowModel =
                new PipelinesDataflowModelParser(
                        new PipelineConfigurationFileReader(scenarioFolder + "/target")
                ).parseConfiguration();

        DynamicPipelineConfigUpdateException exception = org.junit.jupiter.api.Assertions.assertThrows(
                DynamicPipelineConfigUpdateException.class,
                () -> DynamicPipelineUpdateUtil.isDynamicUpdateFeasible(currentPipelinesDataFlowModel, targetPipelinesDataFlowModel)
        );

        if (scenarioFolder.contains("invalid-source-change")) {
            org.junit.jupiter.api.Assertions.assertEquals(SOURCE_CONFIGURATION_ERROR, exception.getMessage());
        } else if (scenarioFolder.contains("invalid-sink-change")) {
            org.junit.jupiter.api.Assertions.assertEquals(SINKS_CONFIGURATION_ERROR, exception.getMessage());
        } else if (scenarioFolder.contains("invalid-processor-change")) {
            // Exception message about single-threaded processor change not allowed
            org.junit.jupiter.api.Assertions.assertEquals(SINGLE_THREADED_PROCESSOR_ERROR, exception.getMessage());
        }
    }

}
