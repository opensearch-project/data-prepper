package org.opensearch.dataprepper.plugins.processor.parse.xml;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.AssertTrue;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import org.opensearch.dataprepper.model.event.HandleFailedEventsOption;
import org.opensearch.dataprepper.plugins.processor.parse.CommonParseConfig;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;

import java.util.List;
import java.util.Objects;

@JsonPropertyOrder
@JsonClassDescription("The <code>parse_xml</code> processor parses XML data for an event.")
public class ParseXmlProcessorConfig implements CommonParseConfig {
    static final String DEFAULT_SOURCE = "message";

    @NotBlank
    @JsonProperty(value = "source", defaultValue = DEFAULT_SOURCE)
    @JsonPropertyDescription("The field in the event that will be parsed. The default value is <code>message</code>.")
    private String source = DEFAULT_SOURCE;

    @JsonProperty("destination")
    @Pattern(regexp = "^(?!\\s*$)(?!^/$).+", message = "Cannot be an empty string, <code>/</code>, or any whitespace-only string")
    @JsonPropertyDescription("The destination field of the structured object from the parsed XML. Defaults to the root of the event. Cannot be an empty string, <code>/</code>, or any whitespace-only string because these are not valid event fields.")
    private String destination;

    @JsonProperty("pointer")
    @JsonPropertyDescription("A JSON pointer to the field to be parsed. There is no pointer by default, meaning the entire source is parsed. The pointer can access JSON array indexes as well. " +
            "If the JSON pointer is invalid then the entire source data is parsed into the outgoing event. If the key that is pointed to already exists in the event and the destination is the root, then the pointer uses the entire path of the key.")
    @ExampleValues({
        @Example(value = "/example/pointer", description = "The field at '/example/pointer' in the input data will be parsed if the pointer is valid.")
    })
    private String pointer;

    @JsonProperty(value = "overwrite_if_destination_exists", defaultValue = "true")
    @JsonPropertyDescription("Overwrites the destination if set to true. Set to false to prevent changing a destination value that exists. Defaults to true.")
    private boolean overwriteIfDestinationExists = true;

    @JsonProperty
    @JsonPropertyDescription("If true, the configured <code>source</code> field will be deleted after the XML data is parsed into separate fields.")
    private boolean deleteSource = false;

    @JsonProperty("parse_when")
    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a> such as <code>/some_key == \"test\"</code>. " +
            "If specified, the <code>parse_xml</code> processor will only run on events when the expression evaluates to true. ")
    @ExampleValues({
        @Example(value = "/some_key == null", description = "Only runs parsing on the Event if some_key is null or doesn't exist.")
    })
    private String parseWhen;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("A list of strings specifying the tags to be set in the event when the processor fails or an unknown exception occurs during parsing.")
    private List<String> tagsOnFailure;

    @JsonProperty("handle_failed_events")
    @JsonPropertyDescription("Determines how to handle events with XML processing errors. Options include 'skip', " +
            "which will log the error and send the event downstream to the next processor, and 'skip_silently', " +
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
    public int getDepth() { return 0; }

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
