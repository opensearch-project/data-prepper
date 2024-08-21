/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The `select_entries` processor selects entries from a Data Prepper event.")
public class SelectEntriesProcessorConfig {
    @NotEmpty
    @NotNull
    @JsonProperty("include_keys")
    @JsonPropertyDescription("A list of keys to be selected from an event.")
    private List<String> includeKeys;

    @JsonProperty("select_when")
    @JsonPropertyDescription("A Data Prepper [conditional expression](https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/), " +
            "such as `/some-key == \"test\"'`, that will be evaluated to determine whether the processor will be " +
            "run on the event. Default is `null`. All events will be processed unless otherwise stated.")
    private String selectWhen;

    public List<String> getIncludeKeys() {
        return includeKeys;
    }

    public String getSelectWhen() {
        return selectWhen;
    }
}

