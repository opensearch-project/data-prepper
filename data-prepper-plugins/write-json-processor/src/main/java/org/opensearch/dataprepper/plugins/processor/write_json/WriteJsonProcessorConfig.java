/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.write_json;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotNull;

@JsonPropertyOrder
@JsonClassDescription("The `write_json` processor converts an object in an event into a JSON string.")
public class WriteJsonProcessorConfig {
    @JsonProperty("source")
    @NotNull
    private String source;

    @JsonProperty("target")
    private String target;

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }
}
