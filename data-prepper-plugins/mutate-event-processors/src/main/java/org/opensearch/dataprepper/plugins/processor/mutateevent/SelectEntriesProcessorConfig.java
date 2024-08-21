/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The `select_entries` processor selects entries from a Data Prepper event.")
public class SelectEntriesProcessorConfig {
    @NotEmpty
    @NotNull
    @JsonProperty("include_keys")
    private List<String> includeKeys;

    @JsonProperty("select_when")
    private String selectWhen;

    public List<String> getIncludeKeys() {
        return includeKeys;
    }

    public String getSelectWhen() {
        return selectWhen;
    }
}

