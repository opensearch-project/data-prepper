/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.peerforwarder;

import com.amazon.dataprepper.model.event.Event;
import com.amazon.dataprepper.model.record.Record;

import java.util.Collection;

public interface PeerForwarder {
    Collection<Record<Event>> forwardRecords(final Collection<Record<Event>> records);
}
