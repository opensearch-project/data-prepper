package org.opensearch.dataprepper.pipeline.parser.rule;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;

@Builder(setterPrefix = "with")
@Getter
@AllArgsConstructor
public class RuleEvaluatorResult {

    private boolean evaluatedResult;

    private String pipelineName;

    public RuleEvaluatorResult() {

    }
}
