/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parsejson;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;

import java.util.Objects;

public class ParseJsonProcessorConfig {
    static final String DEFAULT_SOURCE = "message";

    @NotBlank
    @JsonProperty("source")
    private String source = DEFAULT_SOURCE;

    @JsonProperty("destination")
    private String destination;

    @JsonProperty("pointer")
    private String pointer;

    @JsonProperty("parse_when")
    private String parseWhen;

    /**
     * The field of the Event that contains the JSON data.
     *
     * @return The name of the source field.
     */
    public String getSource() {
        return source;
    }

    /**
     * The destination that the parsed JSON is written to. Defaults to the root of the Event.
     * If the destination field already exists, it will be overwritten.
     *
     * @return The name of the destination field.
     */
    public String getDestination() {
        return destination;
    }

    /**
     * An optional setting used to specify a JSON Pointer. Pointer points to the JSON key that will be parsed into the destination.
     * There is no pointer by default, meaning that the entirety of source will be parsed. If the target key would overwrite an existing
     * key in the Event then the absolute path of the target key will be placed into destination
     *
     * Note: (should this be configurable/what about double conflicts?)
     * @return String representing JSON Pointer
     */
    public String getPointer() {
        return pointer;
    }

    public String getParseWhen() { return parseWhen; }

    @AssertTrue(message = "destination cannot be empty, whitespace, or a front slash (/)")
    boolean isValidDestination() {
        if (Objects.isNull(destination)) return true;

        final String trimmedDestination = destination.trim();
        return trimmedDestination.length() != 0 && !(trimmedDestination.equals("/"));
    }
}