/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.rule;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import org.opensearch.dataprepper.pipeline.parser.transformer.PipelineTemplateModel;

@Builder(setterPrefix = "with")
@Getter
@AllArgsConstructor
public class RuleEvaluatorResult {

    private boolean evaluatedResult;

    private String pipelineName;

    private PipelineTemplateModel pipelineTemplateModel;

    public RuleEvaluatorResult() {

    }
}
