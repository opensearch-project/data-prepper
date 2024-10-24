/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.rule;

import org.junit.jupiter.api.Test;
import org.opensearch.dataprepper.pipeline.parser.transformer.PipelineTemplateModel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RuleEvaluatorResultTest {

    @Test
    void builder_createsObjectCorrectly() {
        PipelineTemplateModel pipelineTemplateModel = new PipelineTemplateModel();
        RuleEvaluatorResult result = RuleEvaluatorResult.builder()
                .withEvaluatedResult(true)
                .withPipelineName("testPipeline")
                .withPipelineTemplateModel(pipelineTemplateModel)
                .build();

        assertTrue(result.isEvaluatedResult());
        assertEquals(result.getPipelineName(), "testPipeline");
        assertEquals(result.getPipelineTemplateModel(), pipelineTemplateModel);
    }

    @Test
    void defaultConstructor_initializesFieldsCorrectly() {
        RuleEvaluatorResult result = new RuleEvaluatorResult();
        assertFalse(result.isEvaluatedResult());
        assertNull(result.getPipelineName());
        assertNull(result.getPipelineTemplateModel());
    }

    @Test
    void allArgsConstructor_assignsFieldsCorrectly() {
        PipelineTemplateModel pipelineTemplateModel = new PipelineTemplateModel();
        RuleEvaluatorResult result = new RuleEvaluatorResult(true,
                "testPipeline", pipelineTemplateModel);

        assertTrue(result.isEvaluatedResult());
        assertEquals(result.getPipelineName(), "testPipeline");
        assertEquals(result.getPipelineTemplateModel(), pipelineTemplateModel);
    }
}
