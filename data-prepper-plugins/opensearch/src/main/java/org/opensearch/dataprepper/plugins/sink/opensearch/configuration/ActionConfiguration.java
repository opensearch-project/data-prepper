/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.sink.opensearch.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import lombok.Getter;

public class ActionConfiguration {
    @NotEmpty
    @JsonProperty("type")
    private String type;

    public String getType() {
        return type;
    }

    @Getter
    @JsonProperty("when")
    private String when;

}
