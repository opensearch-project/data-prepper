/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;

import java.util.List;

public class ObfuscationProcessorConfig {

    @JsonProperty("source")
    @JsonPropertyDescription("The source field to obfuscate.")
    @NotEmpty
    @NotNull
    private String source;

    @JsonProperty("patterns")
    private List<String> patterns;
    
    @JsonProperty("target")
    @JsonPropertyDescription("The new field in which to store the obfuscated value. " +
            "This leaves the original source field unchanged. " +
            "When no `target` is provided, the source field updates with the obfuscated value.")
    private String target;

    @JsonProperty("action")
    @JsonPropertyDescription("The obfuscation action. As of Data Prepper 2.3, only the `mask` action is supported.")
    private PluginModel action;

    @JsonProperty("obfuscate_when")
    @JsonPropertyDescription("Specifies under what condition the Obfuscate processor should perform matching. " +
            "Default is no condition.")
    private String obfuscateWhen;

    @JsonProperty("tags_on_match_failure")
    @JsonPropertyDescription("The tag to add to an event if the obfuscate processor fails to match the pattern.")
    private List<String> tagsOnMatchFailure;

    @JsonProperty("single_word_only")
    @JsonPropertyDescription("When set to `true`, a word boundary `\b` is added to the pattern, " +
            "which causes obfuscation to be applied only to words that are standalone in the input text. " +
            "By default, it is false, meaning obfuscation patterns are applied to all occurrences. " +
            "Can be used for Data Prepper 2.8 or greater.")
    private boolean singleWordOnly = false;

    public ObfuscationProcessorConfig() {
    }

    public ObfuscationProcessorConfig(String source, List<String> patterns, String target, PluginModel action, List<String> tagsOnMatchFailure, boolean singleWordOnly) {
        this.source = source;
        this.patterns = patterns;
        this.target = target;
        this.action = action;
        this.tagsOnMatchFailure = tagsOnMatchFailure;
        this.singleWordOnly = singleWordOnly;
    }

    public String getSource() {
        return source;
    }

    public List<String> getPatterns() {
        return patterns;
    }

    public String getTarget() {
        return target;
    }

    public PluginModel getAction() {
        return action;
    }

    public String getObfuscateWhen() {
        return obfuscateWhen;
    }

    public List<String> getTagsOnMatchFailure() {
        return tagsOnMatchFailure;
    }

    public boolean getSingleWordOnly() {
        return singleWordOnly;
    }

    void validateObfuscateWhen(final ExpressionEvaluator expressionEvaluator) {
        if (obfuscateWhen != null && !expressionEvaluator.isValidExpressionStatement(obfuscateWhen)) {
            throw new InvalidPluginConfigurationException(String.format("obfuscate_when value %s is not a valid Data Prepper expression statement", obfuscateWhen));
        }
    }
}
