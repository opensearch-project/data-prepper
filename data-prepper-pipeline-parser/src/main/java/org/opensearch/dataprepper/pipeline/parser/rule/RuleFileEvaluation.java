package org.opensearch.dataprepper.pipeline.parser.rule;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder(setterPrefix = "with")
@AllArgsConstructor
@Data
public class RuleFileEvaluation {
    private Boolean result;
    private String ruleFileName;
    private String pluginName;

    public RuleFileEvaluation() {

    }
}
