/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.model.peerforwarder;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;

/**
 * An interface that a {@link org.opensearch.dataprepper.model.processor.Processor} will implement which must have peer forwarding prior to processing events.
 * @since 2.0
 */
public interface RequiresPeerForwarding {
    /**
     * Gets the identification keys which Peer Forwarder uses to allocate Events to specific Data Prepper nodes.
     *
     * @return A set of keys
     */
    Collection<String> getIdentificationKeys();

    /**
     * Returns events that are applicable for peer forwarding.
     *
     * @param records collection of input records
     *
     * @return a collection of output records
     */
    default Collection<Record<Event>> applicableEventsForPeerForwarding(Collection<Record<Event>> records) {
        return records;
    }
}
