/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;

/**
 * Interface to handle forwarding records and receiving records from peers
 *
 * @since 2.0
 */
public interface PeerForwarder {
    /**
     * Forwards records to peers
     * @param records collections of records to forward
     * @return collection of records to process locally
     */
    Collection<Record<Event>> forwardRecords(final Collection<Record<Event>> records);

    /**
     * Receives records from {@link PeerForwarderReceiveBuffer} forwarded by peers and process them locally
     * @return collection of records forwarded by peers
     */
    Collection<Record<Event>> receiveRecords();
}
