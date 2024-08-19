/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.drop;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotEmpty;
import org.opensearch.dataprepper.model.event.HandleFailedEventsOption;

public class DropEventProcessorConfig {
    @JsonProperty("drop_when")
    @NotEmpty
    private String dropWhen;

    @JsonProperty("handle_failed_events")
    private HandleFailedEventsOption handleFailedEventsOption = HandleFailedEventsOption.SKIP;

    public String getDropWhen() {
        return dropWhen;
    }

    public HandleFailedEventsOption getHandleFailedEventsOption() {
        return handleFailedEventsOption;
    }
}
