package org.opensearch.dataprepper.plugins.processor.parse.xml;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.event.HandleFailedEventsOption;
import org.opensearch.dataprepper.plugins.processor.parse.CommonParseConfig;

import java.util.List;
import java.util.Objects;

public class ParseXmlProcessorConfig implements CommonParseConfig {
    static final String DEFAULT_SOURCE = "message";

    @NotBlank
    @JsonProperty("source")
    @JsonPropertyDescription("The field in the event that will be parsed. Default value is message.")
    private String source = DEFAULT_SOURCE;

    @JsonProperty("destination")
    @JsonPropertyDescription("The destination field of the parsed JSON. Defaults to the root of the event. Cannot be an empty string, /, or any white-space-only string because these are not valid event fields.")
    private String destination;

    @JsonProperty("pointer")
    @JsonPropertyDescription("A JSON pointer to the field to be parsed. There is no pointer by default, meaning the entire source is parsed. The pointer can access JSON array indexes as well. If the JSON pointer is invalid then the entire source data is parsed into the outgoing event. If the key that is pointed to already exists in the event and the destination is the root, then the pointer uses the entire path of the key.")
    private String pointer;

    @JsonProperty("parse_when")
    @JsonPropertyDescription("A Data Prepper [conditional expression](https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/), such as '/some-key == \"test\"', that will be evaluated to determine whether the processor will be run on the event.")
    private String parseWhen;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("A list of strings specifying the tags to be set in the event that the processor fails or an unknown exception occurs during parsing.")
    private List<String> tagsOnFailure;

    @JsonProperty("overwrite_if_destination_exists")
    @JsonPropertyDescription("Overwrites the destination if set to true. Set to false to prevent changing a destination value that exists. Defaults to true.")
    private boolean overwriteIfDestinationExists = true;

    @JsonProperty
    @JsonPropertyDescription("If true, the configured source field will be deleted after the JSON data is parsed into separate fields.")
    private boolean deleteSource = false;

    @JsonProperty("handle_failed_events")
    @JsonPropertyDescription("Determines how to handle events with XML processing errors. Options include 'skip', " +
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
    public String getParseWhen() {
        return parseWhen;
    }

    @Override
    public boolean getOverwriteIfDestinationExists() {
        return overwriteIfDestinationExists;
    }

    @AssertTrue(message = "destination cannot be empty, whitespace, or a front slash (/)")
    boolean isValidDestination() {
        if (Objects.isNull(destination)) return true;

        final String trimmedDestination = destination.trim();
        return !trimmedDestination.isEmpty() && !(trimmedDestination.equals("/"));
    }

    @Override
    public boolean isDeleteSourceRequested() {
        return deleteSource;
    }

    @Override
    public HandleFailedEventsOption getHandleFailedEventsOption() {
        return handleFailedEventsOption;
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
