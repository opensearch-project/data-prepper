/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;

import java.util.List;

@JsonPropertyOrder
@JsonClassDescription("The <code>map_entries</code> processor wraps each element of a primitive array " +
        "into an object using a configured key name.")
public class MapEntriesProcessorConfig {

    @NotNull
    @NotEmpty
    @JsonProperty("source")
    @JsonPropertyDescription("The key of the primitive array to transform.")
    @ExampleValues({
        @Example(value = "/names", description = "The source array field to wrap into objects.")
    })
    private String source;

    @JsonProperty("target")
    @JsonPropertyDescription("The key to write the resulting object array to. Defaults to <code>source</code> (in-place).")
    @ExampleValues({
        @Example(value = "/agents", description = "Write the resulting object array to a separate field.")
    })
    private String target;

    @NotNull
    @NotEmpty
    @JsonProperty("key")
    @JsonPropertyDescription("The key name to use in each resulting object.")
    @ExampleValues({
        @Example(value = "name", description = "Each primitive value is wrapped as {\"name\": value}.")
    })
    private String key;

    @JsonProperty("exclude_null_empty_values")
    @JsonPropertyDescription("When <code>true</code>, null and empty string elements are filtered out " +
            "before wrapping. Default is <code>false</code>.")
    private boolean excludeNullEmptyValues = false;

    @JsonProperty("append_if_target_exists")
    @JsonPropertyDescription("When <code>true</code>, appends results to the existing target array " +
            "instead of overwriting. Default is <code>false</code>.")
    private boolean appendIfTargetExists = false;

    @JsonProperty("map_entries_when")
    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a> " +
            "that will be evaluated to determine whether the processor will be run on the event.")
    @ExampleValues({
        @Example(value = "/type == \"tagged\"", description = "Only process events where type is 'tagged'.")
    })
    private String mapEntriesWhen;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("A list of tags to add to the event metadata when the event fails to process.")
    private List<String> tagsOnFailure;

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public String getKey() {
        return key;
    }

    public boolean getExcludeNullEmptyValues() {
        return excludeNullEmptyValues;
    }

    public boolean getAppendIfTargetExists() {
        return appendIfTargetExists;
    }

    public String getMapEntriesWhen() {
        return mapEntriesWhen;
    }

    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }

    /**
     * Returns the effective target key. If target is not set, defaults to source.
     */
    public String getEffectiveTarget() {
        return target != null ? target : source;
    }
}
