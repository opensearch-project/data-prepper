/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.client;

import org.opensearch.dataprepper.peerforwarder.PeerForwarder;

/**
 * A class for {@link com.amazon.dataprepper.model.event.EventType} and JSON representation of event data used by {@link PeerForwarder}
 *
 * @since 2.0
 */
class WireEvent {
    private final String eventType;
    private final String eventData;

    public WireEvent(final String eventType, final String eventData) {
        this.eventType = eventType;
        this.eventData = eventData;
    }

    public String getEventType() {
        return eventType;
    }

    public String getEventData() {
        return eventData;
    }
}
