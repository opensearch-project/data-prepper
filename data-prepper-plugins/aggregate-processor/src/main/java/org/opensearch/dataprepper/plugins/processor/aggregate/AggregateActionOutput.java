/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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

