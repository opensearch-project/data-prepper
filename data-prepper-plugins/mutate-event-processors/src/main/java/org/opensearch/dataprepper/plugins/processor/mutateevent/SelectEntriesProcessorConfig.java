/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

public class SelectEntriesProcessorConfig {
    @NotEmpty
    @NotNull
    @JsonProperty("include_keys")
    private String[] includeKeys;

    @JsonProperty("select_when")
    private String selectWhen;

    public String[] getIncludeKeys() {
        return includeKeys;
    }

    public String getSelectWhen() {
        return selectWhen;
    }
}

