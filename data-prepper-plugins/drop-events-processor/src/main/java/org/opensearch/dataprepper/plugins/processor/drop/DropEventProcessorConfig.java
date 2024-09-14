/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.drop;

import com.fasterxml.jackson.annotation.JsonClassDescription;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonPropertyDescription;
import jakarta.validation.constraints.NotEmpty;
import org.opensearch.dataprepper.model.event.HandleFailedEventsOption;

@JsonPropertyOrder
@JsonClassDescription("The `drop_events` processor drops all the events that are passed into it.")
public class DropEventProcessorConfig {

    @JsonPropertyDescription("Accepts a Data Prepper conditional expression string following the [Data Prepper Expression Syntax](https://opensearch.org/docs/latest/data-prepper/pipelines/expression-syntax/). Configuring drop_events with drop_when: true drops all the events received.")
    @JsonProperty("drop_when")
    @NotEmpty
    private String dropWhen;

    @JsonPropertyDescription("Specifies how exceptions are handled when an exception occurs while evaluating an event. Default value is 'drop', which drops the event so that it is not sent to OpenSearch. Available options are 'drop', 'drop_silently', 'skip', and 'skip_silently'.")
    @JsonProperty("handle_failed_events")
    private HandleFailedEventsOption handleFailedEventsOption = HandleFailedEventsOption.SKIP;

    public String getDropWhen() {
        return dropWhen;
    }

    public HandleFailedEventsOption getHandleFailedEventsOption() {
        return handleFailedEventsOption;
    }
}
