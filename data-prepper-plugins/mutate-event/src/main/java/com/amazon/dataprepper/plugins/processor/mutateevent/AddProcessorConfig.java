/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

public class AddProcessorConfig {
    @NotEmpty
    private String key;
    @NotEmpty
    private Object value;
    @JsonProperty("skip_if_present")
    private boolean skipIfPresent;

    public String getKey() {
        return key;
    }

    public Object getValue() {
        return value;
    }

    public boolean getSkipIfPresent() {
        return skipIfPresent;
    }
}
