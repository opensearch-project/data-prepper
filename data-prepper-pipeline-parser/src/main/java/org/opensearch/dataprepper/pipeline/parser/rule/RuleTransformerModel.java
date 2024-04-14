package org.opensearch.dataprepper.pipeline.parser.rule;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.jayway.jsonpath.Predicate;
import lombok.Data;
import lombok.NonNull;
import org.opensearch.dataprepper.pipeline.parser.model.TransformerValueDeserializer;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Data
public class RuleTransformerModel {

    @JsonProperty("apply_when")
    private List<String> applyWhen;

    public RuleTransformerModel() {
    }

    public RuleTransformerModel(List<String> applyWhen) {
        this.applyWhen = applyWhen;
    }

    public List<String> getApplyWhen() {
        return applyWhen;
    }

    public void setApplyWhen(List<String> applyWhen) {
        this.applyWhen = applyWhen;
    }

    @Override
    public String toString() {
        return "RuleConfiguration{" +
                "applyWhen=" + applyWhen +
                '}';
    }
}