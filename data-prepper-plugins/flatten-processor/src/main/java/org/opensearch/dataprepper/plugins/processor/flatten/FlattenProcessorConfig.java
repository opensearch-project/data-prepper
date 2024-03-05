/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.flatten;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.util.ArrayList;
import java.util.List;

public class FlattenProcessorConfig {

    private static final List<String> DEFAULT_EXCLUDE_KEYS = new ArrayList<>();

    @NotNull
    @JsonProperty("source")
    private String source;

    @NotNull
    @JsonProperty("target")
    private String target;

    @JsonProperty("remove_processed_fields")
    private boolean removeProcessedFields = false;

    @JsonProperty("remove_list_indices")
    private boolean removeListIndices = false;

    @JsonProperty("exclude_keys")
    private List<String> excludeKeys = DEFAULT_EXCLUDE_KEYS;

    @JsonProperty("flatten_when")
    private String flattenWhen;

    @JsonProperty("tags_on_failure")
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

    public List<String> getExcludeKeys() {
        return excludeKeys;
    }

    public String getFlattenWhen() {
        return flattenWhen;
    }

    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }
}
