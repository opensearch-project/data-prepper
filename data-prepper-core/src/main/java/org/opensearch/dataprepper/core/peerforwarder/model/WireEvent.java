/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.model;

import org.opensearch.dataprepper.core.peerforwarder.PeerForwarder;

import java.time.Instant;
import java.util.Map;

/**
 * A class for {@link org.opensearch.dataprepper.model.event.EventType} and JSON representation of event data used by {@link PeerForwarder}
 *
 * @since 2.0
 */
public class WireEvent {
    private String eventType;
    private Instant eventTimeReceived;
    private Map<String, Object> eventAttributes;
    private String eventData;

    public WireEvent() {
    }

    // TODO: Add a toJsonString method to EventMetadata and use that instead of metadata fields

    public WireEvent(final String eventType,
                     final Instant eventTimeReceived,
                     final Map<String, Object> eventAttributes,
                     final String eventData) {
        this.eventType = eventType;
        this.eventTimeReceived = eventTimeReceived;
        this.eventAttributes = eventAttributes;
        this.eventData = eventData;
    }

    public String getEventType() {
        return eventType;
    }

    public Instant getEventTimeReceived() {
        return eventTimeReceived;
    }

    public Map<String, Object> getEventAttributes() {
        return eventAttributes;
    }

    public String getEventData() {
        return eventData;
    }
}
