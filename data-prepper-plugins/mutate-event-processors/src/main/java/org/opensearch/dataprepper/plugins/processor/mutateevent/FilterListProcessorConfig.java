/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
@JsonClassDescription("The <code>filter_list</code> processor evaluates a condition against each element of an array " +
        "and keeps only those elements where the condition is true.")
public class FilterListProcessorConfig {

    @NotNull
    @NotEmpty
    @JsonProperty("source")
    @JsonPropertyDescription("The key of the array field to filter. Supports nested paths.")
    @ExampleValues({
            @Example(value = "my-list", description = "Filters the 'my-list' array at the root of the event."),
            @Example(value = "outer-key/my-list", description = "Filters the 'my-list' array nested under 'outer-key'.")
    })
    private String source;

    @JsonProperty("target")
    @JsonPropertyDescription("The key to write the filtered array to. Defaults to the source key (in-place). " +
            "Supports nested paths — intermediate objects are created automatically if they do not exist.")
    @ExampleValues({
            @Example(value = "filtered-list", description = "Writes the filtered array to 'filtered-list'.")
    })
    private String target;

    @NotNull
    @NotEmpty
    @JsonProperty("keep_when")
    @JsonPropertyDescription("An expression evaluated per element. Elements where this expression evaluates to true are kept. " +
            "The expression is evaluated against each element of the array as if it were a standalone event.")
    @ExampleValues({
            @Example(value = "/status == \"active\"", description = "Keeps only elements where 'status' equals 'active'."),
            @Example(value = "/score > 50", description = "Keeps only elements where 'score' is greater than 50.")
    })
    private String keepWhen;

    @JsonProperty("filter_list_when")
    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a>, " +
            "such as <code>/some-key == \"test\"</code>, that will be evaluated against the root event to determine whether " +
            "the processor will be run on the event. By default, all events will be processed unless otherwise stated.")
    @ExampleValues({
            @Example(value = "/some-key == \"test\"", description = "The processor only runs when the value of 'some-key' is 'test'.")
    })
    private String filterListWhen;

    @JsonProperty("tags_on_failure")
    @JsonPropertyDescription("A list of tags to add to the event metadata when the event fails to process.")
    private List<String> tagsOnFailure;

    public String getSource() {
        return source;
    }

    public String getTarget() {
        return target;
    }

    public String getKeepWhen() {
        return keepWhen;
    }

    public String getFilterListWhen() {
        return filterListWhen;
    }

    public List<String> getTagsOnFailure() {
        return tagsOnFailure;
    }
}
