/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.peerforwarder;

import java.util.Set;

/**
 * Add this interface to a Processor which must have peer forwarding
 * prior to processing events.
 */
public interface RequiresPeerForwarding {
    /**
     * Gets the identification keys which Peer Forwarder uses to allocate
     * Events to specific Data Prepper nodes.
     *
     * @return A set of keys
     */
    Set<String> getIdentificationKeys();
}
