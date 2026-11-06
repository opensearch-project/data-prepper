/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.pipeline.parser.rule;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.pipeline.parser.transformer.PipelineTemplateModel;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuleEvaluatorResultTest {

    @Test
    void builder_createsObjectCorrectly() {
        PipelineTemplateModel pipelineTemplateModel = new PipelineTemplateModel();
        List<String> functionProviders = Arrays.asList("org.example.Functions");

        RuleEvaluatorResult result = RuleEvaluatorResult.builder()
                .withEvaluatedResult(true)
                .withPipelineName("testPipeline")
                .withPipelineTemplateModel(pipelineTemplateModel)
                .withFunctionProviders(functionProviders)
                .build();

        assertTrue(result.isEvaluatedResult());
        assertEquals("testPipeline", result.getPipelineName());
        assertEquals(pipelineTemplateModel, result.getPipelineTemplateModel());
        assertEquals(functionProviders, result.getFunctionProviders());
    }

    @Test
    void builder_withNullFunctionProviders_setsNull() {
        RuleEvaluatorResult result = RuleEvaluatorResult.builder()
                .withEvaluatedResult(false)
                .withPipelineName(null)
                .withPipelineTemplateModel(null)
                .withFunctionProviders(null)
                .build();

        assertFalse(result.isEvaluatedResult());
        assertNull(result.getPipelineName());
        assertNull(result.getPipelineTemplateModel());
        assertNull(result.getFunctionProviders());
    }

    @Test
    void defaultConstructor_initializesFieldsCorrectly() {
        RuleEvaluatorResult result = new RuleEvaluatorResult();
        assertFalse(result.isEvaluatedResult());
        assertNull(result.getPipelineName());
        assertNull(result.getPipelineTemplateModel());
        assertNull(result.getFunctionProviders());
    }

    @Test
    void allArgsConstructor_assignsFieldsCorrectly() {
        PipelineTemplateModel pipelineTemplateModel = new PipelineTemplateModel();
        List<String> functionProviders = Arrays.asList("com.example.Provider");

        RuleEvaluatorResult result = new RuleEvaluatorResult(true,
                "testPipeline", pipelineTemplateModel, functionProviders);

        assertTrue(result.isEvaluatedResult());
        assertEquals("testPipeline", result.getPipelineName());
        assertEquals(pipelineTemplateModel, result.getPipelineTemplateModel());
        assertEquals(functionProviders, result.getFunctionProviders());
    }
}
