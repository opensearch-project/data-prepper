/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;

import java.util.List;

public class KeyConfig {

    @JsonProperty("source")
    @NotNull
    String source;

    @JsonProperty("target")
    String target;

    @JsonProperty("attributes")
    List<String> attributes;

    /**
     * Get the Configured source for extracting the IP
     * @return String
     */
    public String getSource() { return source; }

    /**
     * Get the Configured target name
     * @return String
     */
    public String getTarget() {
        return target;
    }

    /**
     * Get the list of Configured attributes
     * @return List of Strings
     */
    public List<String> getAttributes() {
        return attributes;
    }
}
