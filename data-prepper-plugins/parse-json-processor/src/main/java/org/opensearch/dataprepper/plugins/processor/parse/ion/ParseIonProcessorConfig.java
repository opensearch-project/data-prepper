/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.parse.ion;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;
import org.opensearch.dataprepper.model.event.HandleFailedEventsOption;
import org.opensearch.dataprepper.plugins.processor.parse.CommonParseConfig;

import java.util.List;
import java.util.Objects;

@JsonPropertyOrder
@JsonClassDescription("The <code>parse_ion</code> processor parses <a href=\"https://amazon-ion.github.io/ion-docs/\">Amazon Ion</a> data.")
public class ParseIonProcessorConfig implements CommonParseConfig {
    static final String DEFAULT_SOURCE = "message";

    @NotBlank
    @JsonProperty("source")
    @JsonPropertyDescription("The field in the event that will be parsed. The default value is <code>message</code>.")
    private String source = DEFAULT_SOURCE;

    @JsonProperty("destination")
    @Pattern(regexp = "^(?!\\s*$)(?!^/$).+", message = "Cannot be an empty string, <code>/</code>, or any whitespace-only string")
    @JsonPropertyDescription("The destination field of the structured object from the parsed ION. Defaults to the root of the event. Cannot be an empty string, <code>/</code>, or any whitespace-only string because these are not valid event fields.")
    private String destination;

    @JsonProperty("pointer")
    @JsonPropertyDescription("A JSON pointer to the field to be parsed. There is no pointer by default, meaning the entire source is parsed. The pointer can access JSON array indexes as well. " +
            "If the JSON pointer is invalid then the entire source data is parsed into the outgoing event. If the key that is pointed to already exists in the event and the destination is the root, then the pointer uses the entire path of the key.")
    private String pointer;

    @JsonProperty(value = "overwrite_if_destination_exists", defaultValue = "true")
    @JsonPropertyDescription("Overwrites the destination if set to true. Set to false to prevent changing a destination value that exists. Defaults to true.")
    private boolean overwriteIfDestinationExists = true;

    @JsonProperty
    @JsonPropertyDescription("If true, the configured <code>source</code> field will be deleted after the ION data is parsed into separate fields.")
    private boolean deleteSource = false;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("A list of strings specifying the tags to be set in the event that the processor fails or an unknown exception occurs during parsing.")
    private List<String> tagsOnFailure;

    @JsonProperty("parse_when")
    @JsonPropertyDescription("A Data Prepper [conditional expression](https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/), such as '/some-key == \"test\"', that will be evaluated to determine whether the processor will be run on the event.")
    private String parseWhen;

    @JsonProperty("handle_failed_events")
    @JsonPropertyDescription("Determines how to handle events with ION processing errors. Options include 'skip', " +
            "which will log the error and send the event downstream to the next processor, and 'skip_silently', " +
            "which will send the Event downstream to the next processor without logging the error. " +
            "Default is 'skip'.")
    @NotNull
    private HandleFailedEventsOption handleFailedEventsOption = HandleFailedEventsOption.SKIP;

    @JsonProperty("depth")
    @Min(0)
    @Max(10)
    @JsonPropertyDescription("Indicates the depth at which the nested values of the event are not parsed any more. Default is 0, which means all levels of nested values are parsed. If the depth is 1, only the top level keys are parsed and all its nested values are represented as strings")
    private int depth = 0;

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
    public int getDepth() {
        return depth;
    }

    @Override
    public String getParseWhen() { return parseWhen; }

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
