/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder.client;

import org.opensearch.dataprepper.peerforwarder.PeerForwarder;

import java.util.List;

/**
 * A class to serialize lsit of {@link WireEvent} and destination plugin ID used by {@link PeerForwarder}
 *
 * @since 2.0
 */
class WireEvents {
    private final List<WireEvent> events;
    private final String destinationPluginId;

    public WireEvents(final List<WireEvent> events, final String destinationPluginId) {
        this.events = events;
        this.destinationPluginId = destinationPluginId;
    }

    public List<WireEvent> getEvents() {
        return events;
    }

    public String getDestinationPluginId() {
        return destinationPluginId;
    }
}
