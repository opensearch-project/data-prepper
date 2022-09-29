/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.parser;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;
import org.opensearch.dataprepper.parser.model.PipelineConfiguration;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

@RunWith(MockitoJUnitRunner.class)
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
}