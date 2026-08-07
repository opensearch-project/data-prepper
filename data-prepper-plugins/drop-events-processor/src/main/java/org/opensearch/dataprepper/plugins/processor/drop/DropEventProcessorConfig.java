/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.plugins.processor.drop;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotEmpty;
import org.opensearch.dataprepper.model.annotations.ExampleValues;
import org.opensearch.dataprepper.model.annotations.ExampleValues.Example;
import org.opensearch.dataprepper.model.event.HandleFailedEventsOption;

@JsonPropertyOrder
@JsonClassDescription("The <code>drop_events</code> processor conditionally drops events.")
public class DropEventProcessorConfig {

    @JsonPropertyDescription("A <a href=\"https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/\">conditional expression</a> such as <code>/log_type == \"DEBUG\"</code>. " +
            "The <code>drop_when</code> processor will drop all events where the condition evaluates to true. Those events will not go to any further processors or sinks.")
    @JsonProperty("drop_when")
    @NotEmpty
    @ExampleValues({
        @Example(value = "/log_type == \"DEBUG\"", description = "Drops events if the log type is DEBUG.")
    })
    private String dropWhen;

    @JsonPropertyDescription("Specifies how exceptions are handled when an exception occurs while evaluating an event. Default value is <code>skip</code>, which sends the events to further processors or sinks.")
    @JsonProperty(value = "handle_failed_events", defaultValue = "skip")
    private HandleFailedEventsOption handleFailedEventsOption = HandleFailedEventsOption.SKIP;

    public String getDropWhen() {
        return dropWhen;
    }

    public HandleFailedEventsOption getHandleFailedEventsOption() {
        return handleFailedEventsOption;
    }
}
