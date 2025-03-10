/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.configuration.ConditionalRoute;
import org.opensearch.dataprepper.model.sink.SinkContext;
import org.opensearch.dataprepper.pipeline.parser.model.PipelineConfiguration;
import org.opensearch.dataprepper.pipeline.parser.model.SinkContextPluginSetting;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PipelineConfigurationValidatorTest {

    @Test
    void test_with_invalid_pipeline_names_should_throw() {
        final Map<String, PipelineConfiguration> objectObjectHashMap = new HashMap<>();

        objectObjectHashMap.put("core", mock(PipelineConfiguration.class));
        objectObjectHashMap.put("data-prepper", mock(PipelineConfiguration.class));

        assertThrows(RuntimeException.class, () -> PipelineConfigurationValidator.validateAndGetPipelineNames(objectObjectHashMap));
    }

    @Test
    void test_with_valid_pipeline_names_should_not_throw() {
        final Map<String, PipelineConfiguration> objectObjectHashMap = new HashMap<>();

        objectObjectHashMap.put("entry-pipeline", mock(PipelineConfiguration.class));

        PipelineConfigurationValidator.validateAndGetPipelineNames(objectObjectHashMap);
    }

    @Test
    void test_with_invalid_sink_routes_should_throw() {
        PipelineConfiguration pipelineConfig = mock(PipelineConfiguration.class);
        SinkContextPluginSetting sinkSetting = mock(SinkContextPluginSetting.class);
        SinkContext sinkContext = mock(SinkContext.class);

        when(sinkSetting.getSinkContext()).thenReturn(sinkContext);
        when(sinkContext.getRoutes()).thenReturn(List.of("INVALID_ROUTE"));
        when(pipelineConfig.getSinkPluginSettings()).thenReturn(List.of(sinkSetting));
        when(pipelineConfig.getRoutes()).thenReturn(Set.of(new ConditionalRoute("VALID_ROUTE", "condition")));

        Map<String, PipelineConfiguration> pipelineConfigMap = Map.of("test-pipeline", pipelineConfig);

        RuntimeException exception = assertThrows(RuntimeException.class,
                () -> PipelineConfigurationValidator.validateAndGetPipelineNames(pipelineConfigMap));

        assertEquals("The following routes do not exist in pipeline \"test-pipeline\": [INVALID_ROUTE]. " +
                "Configured routes include [VALID_ROUTE]", exception.getMessage());
    }

    @Test
    void test_with_valid_sink_routes_should_not_throw() {
        PipelineConfiguration pipelineConfig = mock(PipelineConfiguration.class);
        SinkContextPluginSetting sinkSetting = mock(SinkContextPluginSetting.class);
        SinkContext sinkContext = mock(SinkContext.class);
        PipelineConfiguration targetPipelineConfig = mock(PipelineConfiguration.class);
        PluginSetting targetSourceSetting = mock(PluginSetting.class);

        when(sinkSetting.getName()).thenReturn("pipeline");
        when(sinkSetting.getAttributeFromSettings("name")).thenReturn("target-pipeline");
        when(sinkContext.getRoutes()).thenReturn(List.of("VALID_ROUTE"));
        when(sinkSetting.getSinkContext()).thenReturn(sinkContext);
        when(pipelineConfig.getSinkPluginSettings()).thenReturn(List.of(sinkSetting));
        when(pipelineConfig.getRoutes()).thenReturn(Set.of(new ConditionalRoute("VALID_ROUTE", "condition")));

        when(targetSourceSetting.getName()).thenReturn("pipeline");
        when(targetSourceSetting.getAttributeFromSettings("name")).thenReturn("test-pipeline");
        when(targetPipelineConfig.getSourcePluginSetting()).thenReturn(targetSourceSetting);

        Map<String, PipelineConfiguration> pipelineConfigMap = new HashMap<>();
        pipelineConfigMap.put("test-pipeline", pipelineConfig);
        pipelineConfigMap.put("target-pipeline", targetPipelineConfig);

        assertDoesNotThrow(() -> PipelineConfigurationValidator.validateAndGetPipelineNames(pipelineConfigMap));
    }
}