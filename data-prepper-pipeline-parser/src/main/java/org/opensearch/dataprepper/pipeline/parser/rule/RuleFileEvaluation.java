package org.opensearch.dataprepper.pipeline.parser.rule;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.util.List;

@Builder(setterPrefix = "with")
@AllArgsConstructor
@Data
public class RuleFileEvaluation {
    private Boolean result;
    private String ruleFileName;
    private String pluginName;
    private List<String> functionProviders;

    public RuleFileEvaluation() {

    }
}
