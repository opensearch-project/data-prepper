/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.obfuscation;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.expression.ExpressionEvaluator;
import org.opensearch.dataprepper.model.annotations.UsesDataPrepperPlugin;
import org.opensearch.dataprepper.model.configuration.PluginModel;
import org.opensearch.dataprepper.model.plugin.InvalidPluginConfigurationException;
import org.opensearch.dataprepper.plugins.processor.obfuscation.action.ObfuscationAction;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The <code>obfuscate</code> process enables obfuscation of fields inside your documents in order to " +
        "protect sensitive data.")
public class ObfuscationProcessorConfig {

    @JsonProperty("source")
    @JsonPropertyDescription("The source field to obfuscate.")
    @NotEmpty
    @NotNull
    private String source;

    @JsonProperty("target")
    @JsonPropertyDescription("The new field in which to store the obfuscated value. " +
            "This leaves the original source field unchanged. " +
            "When no <code>target</code> is provided, the source field updates with the obfuscated value.")
    private String target;

    @JsonProperty("patterns")
    @JsonPropertyDescription("A list of regex patterns that allow you to obfuscate specific parts of a field. Only parts that match the regex pattern will obfuscate. When not provided, the processor obfuscates the whole field.")
    private List<String> patterns;

    @JsonProperty("action")
    @JsonPropertyDescription("The obfuscation action. Available actions include <code>hash</code> and <code>mask</code>.")
    @UsesDataPrepperPlugin(pluginType = ObfuscationAction.class)
    private PluginModel action;

    @JsonProperty("single_word_only")
    @JsonPropertyDescription("When set to <code>true</code>, a word boundary <code>\b</code> is added to the pattern, " +
            "which causes obfuscation to be applied only to words that are standalone in the input text. " +
            "By default, it is false, meaning obfuscation patterns are applied to all occurrences.")
    private boolean singleWordOnly = false;

    @JsonProperty("tags_on_match_failure")
    @JsonPropertyDescription("The tag to add to an event if the <code>obfuscate</code> processor fails to match the pattern.")
    private List<String> tagsOnMatchFailure;

    @JsonProperty("obfuscate_when")
    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a> such as <code>/is_testing_data == true</code>. " +
            "If specified, the <code>obfuscate</code> processor will only run on events when the expression evaluates to true. ")
    private String obfuscateWhen;

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
            throw new InvalidPluginConfigurationException(String.format("obfuscate_when value %s is not a valid Data Prepper expression statement. See https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/ for valid expression syntax", obfuscateWhen));
        }
    }
}
