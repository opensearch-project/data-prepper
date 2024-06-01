/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.dataprepper.pipeline.parser.rule;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

@Data
@AllArgsConstructor
public class RuleTransformerModel {

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    @JsonProperty("apply_when")
    private List<String> applyWhen;

    @JsonProperty("plugin_name")
    private String pluginName;

    public RuleTransformerModel() {
    }

    @Override
    public String toString() {
        return "RuleConfiguration{" +
                "applyWhen=" + applyWhen +
                "\npluginName="+ pluginName +'}';
    }
}