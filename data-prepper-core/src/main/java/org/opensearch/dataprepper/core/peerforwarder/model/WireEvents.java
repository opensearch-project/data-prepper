/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder.model;

import org.opensearch.dataprepper.core.peerforwarder.PeerForwarder;

import java.util.List;

/**
 * A class to serialize lsit of {@link WireEvent} and destination plugin ID used by {@link PeerForwarder}
 *
 * @since 2.0
 */
public class WireEvents {
    private List<WireEvent> events;
    private String destinationPluginId;
    private String destinationPipelineName;

    public WireEvents() {
    }

    public WireEvents(final List<WireEvent> events, final String destinationPluginId, final String destinationPipelineName) {
        this.events = events;
        this.destinationPluginId = destinationPluginId;
        this.destinationPipelineName = destinationPipelineName;
    }

    public List<WireEvent> getEvents() {
        return events;
    }

    public String getDestinationPluginId() {
        return destinationPluginId;
    }

    public String getDestinationPipelineName() {
        return destinationPipelineName;
    }
}
