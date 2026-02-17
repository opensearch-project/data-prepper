/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
