/*
 * Copyright OpenSearch Contributors
 *  PDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

public class EntryConfig {
    @JsonProperty("source")
    @NotEmpty
    private String source;

    @JsonProperty("target")
    private String target;

    @JsonProperty("fields")
    private List<String> fields;

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public List<String> getFields() {
        return fields;
    }
}
