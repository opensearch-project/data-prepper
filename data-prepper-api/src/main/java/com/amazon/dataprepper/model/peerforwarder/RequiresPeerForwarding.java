/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package com.amazon.dataprepper.model.peerforwarder;

import com.amazon.dataprepper.model.processor.Processor;

import java.util.Set;

/**
 * An interface that a {@link com.amazon.dataprepper.model.processor.Processor} will implement which must have peer forwarding prior to processing events.
 * @since 2.0
 */
public interface RequiresPeerForwarding extends Processor {
    /**
     * Gets the identification keys which Peer Forwarder uses to allocate Events to specific Data Prepper nodes.
     *
     * @return A set of keys
     */
    Set<String> getIdentificationKeys();
}
