/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.aggregate;

import org.opensearch.dataprepper.model.event.Event;
import java.util.List;

public class AggregateActionOutput {

    private final List<Event> events;

    public AggregateActionOutput(List<Event> events) {
        this.events = events;
    }

    public List<Event> getEvents() {
        return events;
    }

}

