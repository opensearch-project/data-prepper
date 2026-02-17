/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dataprepper.core.peerforwarder.model;

import org.opensearch.dataprepper.core.peerforwarder.PeerForwarder;
import org.opensearch.dataprepper.model.event.Event;

import java.io.Serializable;
import java.util.List;

/**
 * A class to serialize list of {@link Event} and destination plugin ID used by {@link PeerForwarder}
 *
 * @since 2.0
 */
public class PeerForwardingEvents implements Serializable {
    private List<Event> events;
    private String destinationPluginId;
    private String destinationPipelineName;

    public PeerForwardingEvents() {
    }

    public PeerForwardingEvents(final List<Event> events, final String destinationPluginId, final String destinationPipelineName) {
        this.events = events;
        this.destinationPluginId = destinationPluginId;
        this.destinationPipelineName = destinationPipelineName;
    }

    public List<Event> getEvents() {
        return events;
    }

    public String getDestinationPluginId() {
        return destinationPluginId;
    }

    public String getDestinationPipelineName() {
        return destinationPipelineName;
    }
}
