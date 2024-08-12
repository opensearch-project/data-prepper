/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.flatten;

import com.fasterxml.jackson.annotation.JsonProperty;

import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;

public class FlattenProcessorConfig {

    private static final List<String> DEFAULT_EXCLUDE_KEYS = new ArrayList<>();

    @NotNull
    @JsonProperty("source")
    @JsonPropertyDescription("The source key on which to perform the operation. If set to an empty string (`\"\"`), " +
            "then the processor uses the root of the event as the source.")
    private String source;

    @NotNull
    @JsonProperty("target")
    @JsonPropertyDescription("The target key to put into the flattened fields. If set to an empty string (`\"\"`), " +
            "then the processor uses the root of the event as the target.")
    private String target;

    @JsonProperty("remove_processed_fields")
    @JsonPropertyDescription("When `true`, the processor removes all processed fields from the source. Default is `false`.")
    private boolean removeProcessedFields = false;

    @JsonProperty("remove_list_indices")
    @JsonPropertyDescription("When `true`, the processor converts the fields from the source map into lists and " +
            "puts the lists into the target field. Default is `false`.")
    private boolean removeListIndices = false;

    @JsonProperty("remove_brackets")
    @JsonPropertyDescription("When `true`, the processor also removes brackets around the indices. Can only be " +
            "set to `true` when `remove_list_indices` is `true`.")
    private boolean removeBrackets = false;

    @JsonProperty("exclude_keys")
    @JsonPropertyDescription("The keys from the source field that should be excluded from processing. " +
            "Default is an empty list (`[]`).")
    private List<String> excludeKeys = DEFAULT_EXCLUDE_KEYS;

    @JsonProperty("flatten_when")
    @JsonPropertyDescription("A Data Prepper conditional expression (https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/), " +
            "such as `/some-key == \"test\"'`, that determines whether the `flatten` processor will be run on the " +
            "event. Default is `null`, which means that all events will be processed unless otherwise stated.")
    private String flattenWhen;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("A list of tags to add to the event metadata when the event fails to process.")
    private List<String> tagsOnFailure;

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
