/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parse.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.event.HandleFailedEventsOption;
import org.opensearch.dataprepper.plugins.processor.parse.CommonParseConfig;

import java.util.Objects;
import java.util.List;

public class ParseJsonProcessorConfig implements CommonParseConfig {
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

    @JsonProperty("tags_on_failure")
    private List<String> tagsOnFailure;

    @JsonProperty("overwrite_if_destination_exists")
    private boolean overwriteIfDestinationExists = true;

    @JsonProperty
    private boolean deleteSource = false;

    @JsonProperty("handle_failed_events")
    @JsonPropertyDescription("Determines how to handle events with JSON processing errors. Options include 'skip', " +
            "which will log the error and send the Event downstream to the next processor, and 'skip_silently', " +
            "which will send the Event downstream to the next processor without logging the error. " +
            "Default is 'skip'.")
    @NotNull
    private HandleFailedEventsOption handleFailedEventsOption = HandleFailedEventsOption.SKIP;

    @Override
    public String getSource() {
        return source;
    }

    @Override
    public String getDestination() {
        return destination;
    }

    @Override
    public String getPointer() {
        return pointer;
    }

    @Override
    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }

    @Override
    public String getParseWhen() { return parseWhen; }

    @Override
    public boolean getOverwriteIfDestinationExists() {
        return overwriteIfDestinationExists;
    }

    @Override
    public boolean isDeleteSourceRequested() {
        return deleteSource;
    }

    @Override
    public HandleFailedEventsOption getHandleFailedEventsOption() {
        return handleFailedEventsOption;
    }

    @AssertTrue(message = "destination cannot be empty, whitespace, or a front slash (/)")
    boolean isValidDestination() {
        if (Objects.isNull(destination)) return true;

        final String trimmedDestination = destination.trim();
        return !trimmedDestination.isEmpty() && !(trimmedDestination.equals("/"));
    }

    @AssertTrue(message = "handled_failed_events must be set to 'skip' or 'skip_silently'.")
    boolean isHandleFailedEventsOptionValid() {
        if (handleFailedEventsOption == null) {
            return true;
        }

        if (handleFailedEventsOption.shouldDropEvent()) {
            return false;
        }

        return true;
    }
}
