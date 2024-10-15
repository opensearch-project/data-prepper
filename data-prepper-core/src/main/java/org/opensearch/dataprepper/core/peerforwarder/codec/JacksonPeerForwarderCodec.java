/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.codec;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.core.peerforwarder.model.PeerForwardingEvents;
import org.opensearch.dataprepper.core.peerforwarder.model.WireEvent;
import org.opensearch.dataprepper.core.peerforwarder.model.WireEvents;
import org.opensearch.dataprepper.model.event.DefaultEventMetadata;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.trace.JacksonSpan;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class JacksonPeerForwarderCodec implements PeerForwarderCodec {
    private static final String TRACE_EVENT_TYPE = "TRACE";

    private final ObjectMapper objectMapper;

    public JacksonPeerForwarderCodec(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public byte[] serialize(final PeerForwardingEvents peerForwardingEvents) throws IOException {
        final WireEvents wireEvents = fromPeerForwardingEventsToWireEvents(peerForwardingEvents);
        return objectMapper.writeValueAsBytes(wireEvents);
    }

    @Override
    public PeerForwardingEvents deserialize(final byte[] bytes) throws IOException {
        final WireEvents wireEvents = objectMapper.readValue(bytes, WireEvents.class);
        return fromWireEventsToPeerForwardingEvents(wireEvents);
    }

    private WireEvents fromPeerForwardingEventsToWireEvents(final PeerForwardingEvents peerForwardingEvents) {
        List<WireEvent> wireEventList = null;
        if (peerForwardingEvents.getEvents() != null) {
            wireEventList = peerForwardingEvents.getEvents().stream().map(event -> new WireEvent(
                    event.getMetadata().getEventType(),
                    event.getMetadata().getTimeReceived(),
                    event.getMetadata().getAttributes(),
                    event.toJsonString()
            )).collect(Collectors.toList());
        }
        return new WireEvents(wireEventList,
                peerForwardingEvents.getDestinationPluginId(), peerForwardingEvents.getDestinationPipelineName());
    }

    private PeerForwardingEvents fromWireEventsToPeerForwardingEvents(final WireEvents wireEvents) {
        List<Event> eventList = null;
        if (wireEvents.getEvents() != null) {
            eventList = wireEvents.getEvents().stream().map(this::transformWireEvent).collect(Collectors.toList());
        }
        return new PeerForwardingEvents(
                eventList, wireEvents.getDestinationPluginId(), wireEvents.getDestinationPipelineName());
    }

    private Event transformWireEvent(final WireEvent wireEvent) {
        final DefaultEventMetadata eventMetadata = getEventMetadata(wireEvent);
        Event event;

        if (wireEvent.getEventType().equalsIgnoreCase(TRACE_EVENT_TYPE)) {
            event = JacksonSpan.builder()
                    .withJsonData(wireEvent.getEventData())
                    .withEventMetadata(eventMetadata)
                    .build();
        } else {
            event = JacksonEvent.builder()
                    .withData(wireEvent.getEventData())
                    .withEventMetadata(eventMetadata)
                    .build();
        }
        return event;
    }

    private DefaultEventMetadata getEventMetadata(final WireEvent wireEvent) {
        return DefaultEventMetadata.builder()
                .withEventType(wireEvent.getEventType())
                .withTimeReceived(wireEvent.getEventTimeReceived())
                .withAttributes(wireEvent.getEventAttributes())
                .build();
    }
}
