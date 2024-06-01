package org.opensearch.dataprepper.pipeline.parser.rule;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Builder(setterPrefix = "with")
@AllArgsConstructor
@Data
public class RuleFileEvaluation {
    Boolean result;
    String ruleFileName;
    String pluginName;

    public RuleFileEvaluation() {

    }
}
