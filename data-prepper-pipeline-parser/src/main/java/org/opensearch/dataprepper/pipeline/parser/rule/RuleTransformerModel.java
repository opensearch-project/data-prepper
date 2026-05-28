/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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

    @JsonProperty("function_providers")
    private List<String> functionProviders;

    public RuleTransformerModel() {
    }

    @Override
    public String toString() {
        return "RuleConfiguration{" +
                "applyWhen=" + applyWhen +
                "\nfunctionProviders=" + functionProviders +
                "\npluginName=" + pluginName + '}';
    }
}
