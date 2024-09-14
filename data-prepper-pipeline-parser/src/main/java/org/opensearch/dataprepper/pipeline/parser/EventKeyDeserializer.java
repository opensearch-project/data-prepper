/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.pipeline.parser;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.opensearch.dataprepper.model.event.EventKey;
import org.opensearch.dataprepper.model.event.EventKeyConfiguration;
import org.opensearch.dataprepper.model.event.EventKeyFactory;

import java.io.IOException;

public class EventKeyDeserializer extends StdDeserializer<EventKey> implements ContextualDeserializer {
    private final EventKeyFactory eventKeyFactory;
    private final EventKeyFactory.EventAction[] eventAction;

    /**
     * Constructs a new {@link EventKeyDeserializer} from an {@link EventKeyFactory}.
     *
     * @param eventKeyFactory The factory for creating {@link EventKey} objects.
     */
    public EventKeyDeserializer(final EventKeyFactory eventKeyFactory) {
        this(eventKeyFactory, new EventKeyFactory.EventAction[] {EventKeyFactory.EventAction.ALL});
    }

    private EventKeyDeserializer(final EventKeyFactory eventKeyFactory, final EventKeyFactory.EventAction[] eventAction) {
        super(EventKey.class);
        this.eventKeyFactory = eventKeyFactory;
        this.eventAction = eventAction;
    }

    @Override
    public EventKey deserialize(final JsonParser parser, final DeserializationContext ctxt) throws IOException {
        final String eventKeyString = parser.getValueAsString();

        return eventKeyFactory.createEventKey(eventKeyString, eventAction);
    }

    @Override
    public JsonDeserializer<?> createContextual(final DeserializationContext deserializationContext, final BeanProperty property) {
        if(property == null)
            return this;

        final EventKeyConfiguration eventKeyConfiguration = property.getAnnotation(EventKeyConfiguration.class);

        if(eventKeyConfiguration == null)
            return this;

        final EventKeyFactory.EventAction[] eventAction = eventKeyConfiguration.value();

        return new EventKeyDeserializer(eventKeyFactory, eventAction);
    }
}
