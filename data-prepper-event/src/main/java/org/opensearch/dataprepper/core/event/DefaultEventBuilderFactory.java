/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.event;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.EventBuilder;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.springframework.stereotype.Component;

@Component
class DefaultEventBuilderFactory extends EventBuilderFactory {
    static final String EVENT_TYPE = "EVENT";

    Class<?> getEventClass() {
        return EventBuilder.class;
    }

    DefaultBaseEventBuilder createNew() {
        return new DefaultEventBuilder();
    }

    public static class DefaultEventBuilder extends DefaultBaseEventBuilder<Event> implements EventBuilder {
        @Override
        String getDefaultEventType() {
            return EVENT_TYPE;
        }

        public Event build() {
            return (Event) JacksonEvent.builder()
                    .withEventMetadata(getEventMetadata())
                    .withData(getData())
                    .build();
        }
    }
}
