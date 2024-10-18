/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.flatten;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.annotations.AlsoRequired;

import java.util.ArrayList;
import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The <code>flatten</code> processor transforms nested objects inside of events into flattened structures.")
public class FlattenProcessorConfig {
    static final String REMOVE_LIST_INDICES_KEY = "remove_list_indices";

    private static final List<String> DEFAULT_EXCLUDE_KEYS = new ArrayList<>();

    @NotNull
    @JsonProperty("source")
    @JsonPropertyDescription("The source key on which to perform the operation. If set to an empty string (<code>\"\"</code>), " +
            "then the processor uses the root of the event as the source.")
    private String source;

    @NotNull
    @JsonProperty("target")
    @JsonPropertyDescription("The target key to put into the flattened fields. If set to an empty string (<code>\"\"</code>) " +
            "then the processor uses the root of the event as the target.")
    private String target;

    @JsonProperty("remove_processed_fields")
    @JsonPropertyDescription("When <code>true</code>, the processor removes all processed fields from the source. " +
            "The default is <code>false</code> which leaves the source fields.")
    private boolean removeProcessedFields = false;

    @JsonProperty(REMOVE_LIST_INDICES_KEY)
    @JsonPropertyDescription("When <code>true</code>, the processor converts the fields from the source map into lists and " +
            "puts the lists into the target field. Default is <code>false</code>.")
    private boolean removeListIndices = false;

    @JsonProperty("remove_brackets")
    @JsonPropertyDescription("When <code>true</code>, the processor also removes brackets around the indices. Can only be " +
            "set to <code>true</code> when <code>remove_list_indices</code> is <code>true</code>.")
    @AlsoRequired(values = {
            @AlsoRequired.Required(name = REMOVE_LIST_INDICES_KEY, allowedValues = {"true"})
    })
    private boolean removeBrackets = false;

    @JsonProperty("exclude_keys")
    @JsonPropertyDescription("The keys from the source field that should be excluded from processing. " +
            "By default no keys are excluded.")
    private List<String> excludeKeys = DEFAULT_EXCLUDE_KEYS;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("A list of tags to add to the event metadata when the event fails to process.")
    private List<String> tagsOnFailure;

    @JsonProperty("flatten_when")
    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a> such as <code>/some_key == \"test\"</code>. " +
            "If specified, the <code>flatten</code> processor will only run on events when the expression evaluates to true. ")
    private String flattenWhen;

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public boolean isRemoveProcessedFields() {
        return removeProcessedFields;
    }

    public boolean isRemoveListIndices() {
        return removeListIndices;
    }

    public boolean isRemoveBrackets() {
        return removeBrackets;
    }

    public List<String> getExcludeKeys() {
        return excludeKeys;
    }

    public String getFlattenWhen() {
        return flattenWhen;
    }

    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }

    @AssertTrue(message = "remove_brackets can not be true if remove_list_indices is false.")
    boolean removeBracketsNotTrueWhenRemoveListIndicesFalse() {
        return (!removeBrackets || removeListIndices);
    }
}
