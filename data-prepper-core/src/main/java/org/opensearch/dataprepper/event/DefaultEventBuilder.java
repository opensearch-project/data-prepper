/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.event;

import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;

public class DefaultEventBuilder extends DefaultBaseEventBuilder<Event> implements EventBuilder {

    static final String EVENT_TYPE = "EVENT";

    public Event build() {
        return (Event) JacksonEvent.builder()
          .withData(getData())
          .withEventType(EVENT_TYPE)
          .build();
    }
}
