/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.write_json;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotNull;

@JsonPropertyOrder
@JsonClassDescription("The `write_json` processor converts an object in an event into a JSON string.")
public class WriteJsonProcessorConfig {
    @JsonProperty("source")
    @JsonPropertyDescription("Specifies the name of the field in the event containing the message or object to be parsed.")
    @NotNull
    private String source;

    @JsonProperty("target")
    @JsonPropertyDescription("Specifies the name of the field in which the resulting JSON string should be stored. If target is not specified, then the source field is used.")
    private String target;

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }
}
