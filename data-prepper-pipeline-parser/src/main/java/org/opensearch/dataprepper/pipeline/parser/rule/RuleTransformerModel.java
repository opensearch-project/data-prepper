/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.rule;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

import java.util.List;

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