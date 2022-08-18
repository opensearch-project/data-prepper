/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

/**
 * A class for {@link com.amazon.dataprepper.model.event.EventType} and JSON representation of event data used by {@link PeerForwarder}
 *
 * @since 2.0
 */
public class SerializedEvent {
    private final String eventType;
    private final String eventData;

    public SerializedEvent(String eventType, String eventData) {
        this.eventType = eventType;
        this.eventData = eventData;
    }
}
