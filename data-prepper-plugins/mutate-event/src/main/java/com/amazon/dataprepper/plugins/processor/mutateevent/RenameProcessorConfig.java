/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

public class RenameProcessorConfig {
    @NotEmpty
    private String from;
    @NotEmpty
    private String to;
    @JsonProperty("skip_if_present")
    private boolean skipIfPresent;

    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    public boolean getSkipIfPresent() {
        return skipIfPresent;
    }
}
