/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import java.util.List;

/**
 * A class to serialize lsit of {@link SerializedEvent} and destination plugin ID used by {@link PeerForwarder}
 *
 * @since 2.0
 */
public class WireEvents {
    private final List<SerializedEvent> events;
    private final String destinationPluginId;

    public WireEvents(List<SerializedEvent> events, String destinationPluginId) {
        this.events = events;
        this.destinationPluginId = destinationPluginId;
    }
}
