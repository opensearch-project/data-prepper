/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.core.peerforwarder;

import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collection;
import java.util.Collections;

public class LocalPeerForwarder implements PeerForwarder {

    @Override
    public Collection<Record<Event>> forwardRecords(final Collection<Record<Event>> records) {
        return records;
    }

    @Override
    public Collection<Record<Event>> receiveRecords() {
        return Collections.emptyList();
    }
}
